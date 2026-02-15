#!/usr/bin/env python3
"""
repair.py

Scan DB for Telegram message references and either clean stale references
or repair missing lecture metadata by refetching captions from Telegram.

Usage:
  python repair.py [--dry-run] [--use-user-session] [--chat-id CHAT_ID]

Notes:
  - Requires TELEGRAM_BOT_TOKEN (or set CLEAN_USE_USER_SESSION=1 to use user session)
  - Uses the same DB layout as other tools in this repo (lecture_uploads, lecture_jobs, backup_id, lectures)
"""

import argparse
import asyncio
import os
import re
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv
load_dotenv()

try:
    from mainLogic.utils import mysql_logger as dbmod
except Exception:
    dbmod = None

from telethon import TelegramClient
from contextlib import contextmanager


@contextmanager
def get_cursor(conn, dict=True):
    try:
        if dict:
            cur = conn.cursor(dictionary=True)
        else:
            cur = conn.cursor()
    except TypeError:
        # Fallback for connectors that don't accept `dictionary` kwarg
        cur = conn.cursor()
    try:
        yield cur
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _parse_caption(text):
    if not text:
        return {}
    subject = chapter = lecture = teacher = None
    for line in text.splitlines():
        if ":" in line:
            label, value = line.split(":", 1)
            label = label.strip().lower()
            value = value.strip()
            if "subject" in label:
                subject = value
            elif "chapter" in label:
                chapter = value
            elif "lecture" in label:
                lecture = value
            elif "teacher" in label or "by" in label:
                teacher = value
        else:
            m = re.match(r"^by\s+(.+)$", line.strip(), flags=re.I)
            if m:
                teacher = m.group(1).strip()
    return {"subject": subject, "chapter": chapter, "lecture": lecture, "teacher": teacher}


def _ensure_teacher_in_caption(caption, teacher_name):
    """Return a caption string where the teacher line is set to teacher_name.
    If a teacher line exists it is replaced, otherwise appended.
    """
    if not caption:
        return f"Teacher: {teacher_name}"
    lines = caption.splitlines()
    found = False
    new_lines = []
    for line in lines:
        if ":" in line:
            label, value = line.split(":", 1)
            if "teacher" in label.strip().lower():
                new_lines.append(f"Teacher: {teacher_name}")
                found = True
                continue
        # weak match: line starts with 'by '
        if re.match(r"^by\s+", line.strip(), flags=re.I):
            new_lines.append(f"Teacher: {teacher_name}")
            found = True
            continue
        new_lines.append(line)
    if not found:
        # add teacher line separated by a blank line for readability
        if new_lines and new_lines[-1].strip() != "":
            new_lines.append("")
        new_lines.append(f"Teacher: {teacher_name}")
    return "\n".join(new_lines)


def _connect_db():
    # prefer existing mysql_logger helper if available
    if dbmod:
        try:
            dbmod.init(None)
        except Exception:
            pass
        return dbmod._connect()

    # fallback: parse PWDL_DB_URL directly
    import mysql.connector
    url = os.environ.get("PWDL_DB_URL")
    if not url:
        raise RuntimeError("PWDL_DB_URL not set")
    p = urlparse(url)
    return mysql.connector.connect(
        host=p.hostname,
        user=p.username,
        password=unquote(p.password or ""),
        database=p.path.lstrip("/"),
        port=p.port or 3306,
    )


def perform_status_fix(conn, dry_run=False):
    """Fix common misspellings of status ('field', 'feild') -> 'uploaded'.
    Returns a dict of table -> affected_rows (or planned rows in dry-run).
    """
    results = {}
    targets = [
        ("lecture_uploads", "status"),
        ("lecture_jobs", "status"),
        ("lectures", "status"),
    ]
    for table, col in targets:
        try:
            with get_cursor(conn, dict=False) as cur:
                # count matching rows first
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IN ('field','feild')")
                    cnt = cur.fetchone()[0]
                except Exception:
                    cnt = 0
                results[table] = cnt
                if cnt and not dry_run:
                    try:
                        cur.execute(f"UPDATE {table} SET {col}='uploaded' WHERE {col} IN ('field','feild')")
                    except Exception as e:
                        print(f"  failed to update {table}: {e}")
        except Exception:
            # table likely doesn't exist or column missing; skip
            results[table] = 0
    if not dry_run:
        try:
            conn.commit()
        except Exception:
            pass
    return results


def perform_clear_failed(conn, dry_run=False):
    """Clear lecture_uploads rows with status='failed'.
    Action: set telegram_chat_id=NULL, telegram_message_id=NULL, telegram_file_id=NULL, status='pending'
    Returns count of affected rows (or planned rows in dry-run).
    """
    results = {"lecture_uploads": 0}
    try:
        with get_cursor(conn, dict=False) as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM lecture_uploads WHERE status='failed'")
                cnt = cur.fetchone()[0]
            except Exception:
                cnt = 0
            results['lecture_uploads'] = cnt
            if cnt and not dry_run:
                try:
                    cur.execute("UPDATE lecture_uploads SET telegram_chat_id=NULL, telegram_message_id=NULL, telegram_file_id=NULL, status='pending' WHERE status='failed'")
                except Exception as e:
                    print(f"  failed to clear lecture_uploads: {e}")
    except Exception:
        results['lecture_uploads'] = 0
    if not dry_run:
        try:
            conn.commit()
        except Exception:
            pass
    return results


async def main(use_user=False, chat_id=None, dry_run=False, limit=None):
    conn = _connect_db()

    api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")

    if use_user and (not api_id or not api_hash):
        raise RuntimeError("User session mode requires TELEGRAM_API_ID/TELEGRAM_API_HASH")

    if use_user:
        client = TelegramClient('repair_user_session', api_id, api_hash)
        await client.start()
    else:
        if not bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN required unless --use-user-session")
        client = TelegramClient('repair_bot_session', api_id, api_hash)
        await client.start(bot_token=bot_token)

    # gather candidate message refs from backup_id, lecture_uploads, lecture_jobs
    candidates = set()
    try:
        has_backup = dbmod.table_exists(conn, 'backup_id') if dbmod else True
    except Exception:
        has_backup = True

    with get_cursor(conn, dict=True) as cur:
        if has_backup:
            try:
                cur.execute("SELECT batch_id, lecture_id, channel_id AS chat_id, message_id FROM backup_id WHERE platform='telegram' AND message_id IS NOT NULL")
                for r in cur.fetchall():
                    if not r.get('message_id'):
                        continue
                    candidates.add((r['batch_id'], r['lecture_id'], str(r['chat_id']), str(r['message_id'])))
            except Exception:
                pass

        try:
            cur.execute("SELECT batch_id, lecture_id, telegram_chat_id AS chat_id, telegram_message_id AS message_id FROM lecture_uploads WHERE telegram_message_id IS NOT NULL")
            for r in cur.fetchall():
                if not r.get('message_id'):
                    continue
                candidates.add((r['batch_id'], r['lecture_id'], str(r['chat_id']), str(r['message_id'])))
        except Exception:
            pass

        try:
            cur.execute("SELECT batch_id, lecture_id, telegram_chat_id AS chat_id, telegram_message_id AS message_id FROM lecture_jobs WHERE telegram_message_id IS NOT NULL")
            for r in cur.fetchall():
                if not r.get('message_id'):
                    continue
                candidates.add((r['batch_id'], r['lecture_id'], str(r['chat_id']), str(r['message_id'])))
        except Exception:
            pass

    # optional filters
    if chat_id:
        candidates = {c for c in candidates if c[2] == str(chat_id)}
    if limit:
        candidates = set(list(candidates)[:limit])

    if not candidates:
        print("No candidate message references found.")
        await client.disconnect()
        conn.close()
        return

    for batch_id, lecture_id, chat_k, msg_k in sorted(candidates):
        print(f"Checking batch={batch_id} lecture={lecture_id} chat={chat_k} msg={msg_k}")
        try:
            msg = await client.get_messages(int(chat_k), ids=int(msg_k))
        except Exception as e:
            print(f"  fetch exception: {e}")
            msg = None

        if not msg or (not getattr(msg, 'message', None) and not getattr(msg, 'text', None)):
            print("  message NOT found -> cleaning DB refs")
            if dry_run:
                continue
            with get_cursor(conn, dict=False) as cur:
                # clear lecture_uploads refs
                try:
                    cur.execute("UPDATE lecture_uploads SET telegram_chat_id=NULL, telegram_message_id=NULL, telegram_file_id=NULL, status='pending' WHERE batch_id=%s AND lecture_id=%s", (batch_id, lecture_id))
                except Exception:
                    pass
                try:
                    cur.execute("UPDATE lecture_jobs SET telegram_chat_id=NULL, telegram_message_id=NULL, telegram_file_id=NULL, status='pending' WHERE batch_id=%s AND lecture_id=%s", (batch_id, lecture_id))
                except Exception:
                    pass
                try:
                    cur.execute("UPDATE backup_id SET message_id=NULL, file_id=NULL WHERE batch_id=%s AND lecture_id=%s AND platform='telegram'", (batch_id, lecture_id))
                except Exception:
                    pass
            conn.commit()
            continue

        # message exists -> attempt metadata repair
        caption = getattr(msg, 'message', None) or getattr(msg, 'text', None) or ''
        parsed = _parse_caption(caption)
        if not any(parsed.values()):
            print("  message present but no parseable caption metadata")
            continue

        # fetch current lecture row
        with get_cursor(conn, dict=True) as cur:
            cur.execute("SELECT subject_name, subject_slug, chapter_name, lecture_name FROM lectures WHERE batch_id=%s AND lecture_id=%s", (batch_id, lecture_id))
            row = cur.fetchone()

        updates = {}
        if row:
            if (not row.get('lecture_name')) and parsed.get('lecture'):
                updates['lecture_name'] = parsed['lecture']
            if (not row.get('chapter_name')) and parsed.get('chapter'):
                updates['chapter_name'] = parsed['chapter']
            if (not row.get('subject_name')) and parsed.get('subject'):
                updates['subject_name'] = parsed['subject']

        if parsed.get('teacher'):
            # try to update lecture_jobs.teacher_names if missing
            with get_cursor(conn, dict=True) as cur:
                cur.execute("SELECT teacher_names FROM lecture_jobs WHERE batch_id=%s AND lecture_id=%s", (batch_id, lecture_id))
                tj = cur.fetchone()
            if tj and not tj.get('teacher_names'):
                if not dry_run:
                    with get_cursor(conn, dict=False) as cur:
                        try:
                            cur.execute("UPDATE lecture_jobs SET teacher_names=%s WHERE batch_id=%s AND lecture_id=%s", (parsed['teacher'], batch_id, lecture_id))
                        except Exception:
                            pass
                    conn.commit()
                print(f"  updated lecture_jobs.teacher_names -> {parsed['teacher']}")

                # Also update the Telegram message caption to show correct teacher
                try:
                    new_caption = _ensure_teacher_in_caption(caption, parsed['teacher'])
                    if new_caption != (caption or ''):
                        if dry_run:
                            print(f"  dry-run edit caption for message {msg_k}")
                        else:
                            try:
                                await client.edit_message(int(chat_k), int(msg_k), new_caption)
                                print(f"  edited telegram message caption for msg {msg_k}")
                            except Exception as e:
                                print(f"  failed to edit message {msg_k}: {e}")
                except Exception as e:
                    print(f"  caption update error: {e}")

        if updates:
            print(f"  will update lectures: {updates}")
            if not dry_run:
                sets = ", ".join([f"{k}=%s" for k in updates.keys()])
                params = list(updates.values()) + [batch_id, lecture_id]
                with get_cursor(conn, dict=False) as cur:
                    try:
                        cur.execute(f"UPDATE lectures SET {sets} WHERE batch_id=%s AND lecture_id=%s", tuple(params))
                    except Exception as e:
                        print(f"  update failed: {e}")
                conn.commit()

    await client.disconnect()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-user-session', action='store_true', help='Use user session (requires TELEGRAM_API_ID/API_HASH)')
    parser.add_argument('--chat-id', help='Optional chat id to restrict scanning')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--fix-status-typos', action='store_true', help="Fix status typos ('field','feild') -> 'uploaded'")
    parser.add_argument('--clear-failed', action='store_true', help="Clear lecture_uploads rows with status 'failed' (set to pending and clear telegram refs)")
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()

    if args.fix_status_typos:
        conn = _connect_db()
        print("Scanning tables for status values 'field' or 'feild'...")
        res = perform_status_fix(conn, dry_run=args.dry_run)
        for t, c in res.items():
            print(f"  {t}: {c} rows {'(would be updated)' if args.dry_run else '(updated)'}")
        conn.close()
    elif args.clear_failed:
        conn = _connect_db()
        print("Scanning lecture_uploads for status='failed'...")
        res = perform_clear_failed(conn, dry_run=args.dry_run)
        for t, c in res.items():
            print(f"  {t}: {c} rows {'(would be cleared)' if args.dry_run else '(cleared)'}")
        conn.close()
    else:
        asyncio.run(main(use_user=args.use_user_session, chat_id=args.chat_id, dry_run=args.dry_run, limit=args.limit))
