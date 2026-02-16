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

try:
    from mainLogic.utils.glv_var import PREFS_FILE
except Exception:
    PREFS_FILE = "preferences.json"

try:
    from beta.batch_scraper_2.Endpoints import Endpoints
except Exception:
    Endpoints = None

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


def _build_caption_from_db(payload, fallback_caption=None):
    if not payload:
        return fallback_caption or ""
    course = payload.get("course_name") or payload.get("batch_slug") or payload.get("batch_id") or ""
    subject = payload.get("subject_name") or payload.get("subject_slug") or ""
    chapter = payload.get("chapter_name") or ""
    lecture = payload.get("lecture_name") or ""
    teacher = payload.get("teacher_names") or ""
    start_time = payload.get("start_time") or ""

    lines = [
        f"Course : {course}",
        f"Subject: {subject}",
        f"Chapter: {chapter}",
        f"Lecture: {lecture}",
    ]
    if teacher:
        lines.append(f"Teacher: {teacher}")
    if start_time:
        lines.append(f"Start  : {start_time}")
    return "\n".join([ln for ln in lines if ln.strip()])


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


def _table_exists(conn, table_name):
    try:
        with get_cursor(conn, dict=False) as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                """,
                (table_name,),
            )
            return cur.fetchone()[0] > 0
    except Exception:
        return False


def _load_token_config():
    try:
        with open(PREFS_FILE, "r", encoding="utf-8") as handle:
            prefs = json.load(handle)
    except Exception:
        return {}
    token = prefs.get("token_config") or prefs.get("token") or {}
    if isinstance(token, dict):
        return token
    return {}


def _init_api():
    if Endpoints is None:
        return None
    token_cfg = _load_token_config()
    access_token = token_cfg.get("access_token") or token_cfg.get("token")
    random_id = token_cfg.get("random_id") or token_cfg.get("randomId")
    if not access_token:
        return None
    if random_id:
        return Endpoints(verbose=False).set_token(access_token, random_id=random_id)
    return Endpoints(verbose=False).set_token(access_token)


def _normalize_tag_name(value):
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _filter_lectures_by_tag_name(lectures, chapter_name):
    if not lectures:
        return []
    target = _normalize_tag_name(chapter_name)
    if not target:
        return []
    filtered = []
    for lecture in lectures:
        tags = getattr(lecture, "tags", None) or []
        for tag in tags:
            tag_name = _normalize_tag_name(getattr(tag, "name", None))
            if tag_name == target:
                filtered.append(lecture)
                break
    return filtered


def _safe_str(value):
    text = str(value).strip() if value is not None else ""
    return text or None


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


async def main(use_user=False, chat_id=None, dry_run=False, limit=None, repair_uploaded=False):
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
    has_backup = _table_exists(conn, 'backup_id')

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

        # message exists -> attempt metadata repair and status repair
        caption = getattr(msg, 'message', None) or getattr(msg, 'text', None) or ''
        parsed = _parse_caption(caption)
        if not any(parsed.values()):
            print("  message present but no parseable caption metadata")

        if repair_uploaded and not dry_run:
            with get_cursor(conn, dict=False) as cur:
                try:
                    cur.execute(
                        """
                        UPDATE lecture_uploads
                        SET status='done', error_text=NULL,
                            telegram_chat_id=%s, telegram_message_id=%s
                        WHERE batch_id=%s AND lecture_id=%s
                        """,
                        (chat_k, msg_k, batch_id, lecture_id),
                    )
                except Exception:
                    pass
                try:
                    cur.execute(
                        """
                        UPDATE lecture_jobs
                        SET status='done', error_text=NULL,
                            telegram_chat_id=%s, telegram_message_id=%s
                        WHERE batch_id=%s AND lecture_id=%s
                        """,
                        (chat_k, msg_k, batch_id, lecture_id),
                    )
                except Exception:
                    pass
                try:
                    cur.execute(
                        """
                        UPDATE backup_id
                        SET message_id=%s
                        WHERE batch_id=%s AND lecture_id=%s AND platform='telegram'
                        """,
                        (msg_k, batch_id, lecture_id),
                    )
                except Exception:
                    pass
            conn.commit()

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

        # Rebuild Telegram caption from DB if requested
        if repair_uploaded and not dry_run:
            payload = None
            with get_cursor(conn, dict=True) as cur:
                try:
                    cur.execute(
                        """
                        SELECT
                            l.batch_id,
                            c.batch_slug,
                            c.name AS course_name,
                            s.slug AS subject_slug,
                            s.name AS subject_name,
                            ch.name AS chapter_name,
                            l.lecture_name,
                            l.start_time,
                            GROUP_CONCAT(t.name ORDER BY t.name SEPARATOR ', ') AS teacher_names
                        FROM lectures l
                        LEFT JOIN courses c ON c.id = l.course_id
                        LEFT JOIN subjects s ON s.id = l.subject_id
                        LEFT JOIN chapters ch ON ch.id = l.chapter_id
                        LEFT JOIN lecture_teachers lt ON lt.batch_id = l.batch_id AND lt.lecture_id = l.lecture_id
                        LEFT JOIN teachers t ON t.id = lt.teacher_id
                        WHERE l.batch_id = %s AND l.lecture_id = %s
                        GROUP BY l.batch_id, c.batch_slug, c.name, s.slug, s.name, ch.name, l.lecture_name, l.start_time
                        """,
                        (batch_id, lecture_id),
                    )
                    payload = cur.fetchone()
                except Exception:
                    payload = None
            new_caption = _build_caption_from_db(payload, fallback_caption=caption)
            if new_caption and new_caption != (caption or ""):
                try:
                    await client.edit_message(int(chat_k), int(msg_k), new_caption)
                    print(f"  updated caption for message {msg_k}")
                except Exception as e:
                    print(f"  failed to update caption {msg_k}: {e}")

    await client.disconnect()
    conn.close()


def sync_chapters_from_api(conn, batch_id=None, batch_slug=None, dry_run=False):
    api = _init_api()
    if api is None:
        print("API token not available; cannot sync chapters.")
        return

    batches = []
    if batch_id or batch_slug:
        batches.append({"batch_id": batch_id, "batch_slug": batch_slug})
    else:
        with get_cursor(conn, dict=True) as cur:
            if _table_exists(conn, "courses"):
                cur.execute("SELECT batch_id, batch_slug FROM courses")
                batches = cur.fetchall() or []
            else:
                cur.execute("SELECT DISTINCT batch_id, batch_slug FROM lecture_jobs")
                batches = cur.fetchall() or []

    for b in batches:
        b_id = b.get("batch_id") if isinstance(b, dict) else None
        b_slug = b.get("batch_slug") if isinstance(b, dict) else None
        if batch_id:
            b_id = batch_id
        if batch_slug:
            b_slug = batch_slug
        if not b_slug:
            print(f"Skipping batch without slug: batch_id={b_id}")
            continue

        print(f"Syncing chapters for batch: {b_slug} ({b_id})")
        try:
            subjects = api.get_batch_details(batch_name=b_slug)
        except Exception as e:
            print(f"  Failed to load subjects: {e}")
            continue

        for subj in subjects or []:
            subject_slug = getattr(subj, "slug", None)
            subject_name = getattr(subj, "name", None) or subject_slug
            subject_id = getattr(subj, "subjectId", None) or getattr(subj, "id", None) or getattr(subj, "_id", None)
            if not subject_slug:
                continue
            try:
                chapters = api.get_batch_subjects(batch_name=b_slug, subject_name=subject_slug)
            except Exception:
                chapters = []

            for chapter in chapters or []:
                chapter_name = getattr(chapter, "name", None)
                if not chapter_name:
                    continue
                tag_id = getattr(chapter, "typeId", None) or getattr(chapter, "id", None) or getattr(chapter, "_id", None)
                lectures = []
                if b_id and subject_id and tag_id and hasattr(api, "get_batch_chapter_lectures_v3"):
                    try:
                        lectures = api.get_batch_chapter_lectures_v3(
                            batch_id=b_id,
                            subject_id=subject_id,
                            tag_id=tag_id,
                        )
                    except Exception:
                        lectures = []
                if not lectures:
                    try:
                        v2 = api.get_batch_chapters(batch_name=b_slug, subject_name=subject_slug, chapter_name=chapter_name)
                        lectures = _filter_lectures_by_tag_name(v2, chapter_name)
                    except Exception:
                        lectures = []

                for lec in lectures:
                    lecture_id = _safe_str(getattr(lec, "id", None))
                    lecture_name = _safe_str(getattr(lec, "name", None))
                    if not lecture_id:
                        continue
                    if dry_run:
                        print(f"  would update lecture {lecture_id} chapter={chapter_name}")
                        continue
                    with get_cursor(conn, dict=False) as cur:
                        try:
                            cur.execute(
                                """
                                UPDATE lectures
                                SET subject_slug=%s, subject_name=%s, chapter_name=%s,
                                    lecture_name=COALESCE(NULLIF(%s,''), lecture_name)
                                WHERE batch_id=%s AND lecture_id=%s
                                """,
                                (
                                    subject_slug,
                                    subject_name,
                                    chapter_name,
                                    lecture_name or "",
                                    b_id,
                                    lecture_id,
                                ),
                            )
                        except Exception:
                            pass
                        try:
                            cur.execute(
                                """
                                UPDATE lecture_jobs
                                SET subject_slug=%s, subject_name=%s, chapter_name=%s,
                                    lecture_name=COALESCE(NULLIF(%s,''), lecture_name)
                                WHERE batch_id=%s AND lecture_id=%s
                                """,
                                (
                                    subject_slug,
                                    subject_name,
                                    chapter_name,
                                    lecture_name or "",
                                    b_id,
                                    lecture_id,
                                ),
                            )
                        except Exception:
                            pass
                    conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-user-session', action='store_true', help='Use user session (requires TELEGRAM_API_ID/API_HASH)')
    parser.add_argument('--chat-id', help='Optional chat id to restrict scanning')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--fix-status-typos', action='store_true', help="Fix status typos ('field','feild') -> 'uploaded'")
    parser.add_argument('--clear-failed', action='store_true', help="Clear lecture_uploads rows with status 'failed' (set to pending and clear telegram refs)")
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--repair-uploaded', action='store_true', help='If message exists, mark DB rows as done and ensure telegram ids are set')
    parser.add_argument('--rebuild-captions', action='store_true', help='Rebuild Telegram captions from DB fields')
    parser.add_argument('--sync-chapters', action='store_true', help='Fetch lecture -> chapter mapping from API and update DB')
    parser.add_argument('--batch-id', help='Optional batch_id to restrict sync')
    parser.add_argument('--batch-slug', help='Optional batch slug to restrict sync')
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
    elif args.sync_chapters:
        conn = _connect_db()
        sync_chapters_from_api(conn, batch_id=args.batch_id, batch_slug=args.batch_slug, dry_run=args.dry_run)
        conn.close()
    else:
        asyncio.run(main(use_user=args.use_user_session, chat_id=args.chat_id, dry_run=args.dry_run, limit=args.limit, repair_uploaded=args.repair_uploaded or args.rebuild_captions))
