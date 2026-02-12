#!/usr/bin/env python3
"""
clean.py

Find duplicate lecture uploads in a Telegram channel and delete extra messages.
Keeps one message per lecture_id per channel (default: latest message_id).
Also updates DB records to point at the kept message_id.

Requires:
- TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH
- TELEGRAM_CHAT_ID (or pass --chat-id)
- PWDL_DB_URL (format: mysql://user:pass@host:port/dbname)
"""

import argparse
import asyncio
import os
import re
from collections import defaultdict
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv
import mysql.connector
from telethon import TelegramClient
import requests

load_dotenv()


# -------------------- DB --------------------

def connect_db():
    db_url = os.environ.get("PWDL_DB_URL")
    if not db_url:
        raise RuntimeError("PWDL_DB_URL not set")

    parsed = urlparse(db_url)
    return mysql.connector.connect(
        host=parsed.hostname,
        user=parsed.username,
        password=unquote(parsed.password),
        database=parsed.path.lstrip("/"),
        port=parsed.port or 3306,
    )


# -------------------- Helpers --------------------

def _normalize_text(value):
    if not value:
        return None
    text = str(value)
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text or None


def _parse_caption(text):
    if not text:
        return {}
    subject = chapter = lecture = None
    for line in text.splitlines():
        if ":" not in line:
            continue
        label, value = line.split(":", 1)
        label = label.strip().lower()
        value = value.strip()
        if "subject" in label:
            subject = value
        elif "chapter" in label:
            chapter = value
        elif "lecture" in label:
            lecture = value
    return {"subject": subject, "chapter": chapter, "lecture": lecture}


def _build_lecture_maps(conn):
    with conn.cursor(dictionary=True) as cur:
        cur.execute(
            """
            SELECT batch_id, lecture_id, subject_name, subject_slug, chapter_name, lecture_name
            FROM lectures
            """
        )
        rows = cur.fetchall()

    by_full = defaultdict(list)
    by_lecture = defaultdict(list)

    for row in rows:
        subject = _normalize_text(row.get("subject_name") or row.get("subject_slug"))
        chapter = _normalize_text(row.get("chapter_name"))
        lecture = _normalize_text(row.get("lecture_name"))
        key_full = (subject, chapter, lecture)
        if lecture:
            by_lecture[lecture].append(row)
        if subject or chapter or lecture:
            by_full[key_full].append(row)
    return by_full, by_lecture


def _pick_unique(rows):
    if not rows:
        return None
    lecture_ids = {str(r.get("lecture_id")) for r in rows if r.get("lecture_id")}
    if len(lecture_ids) != 1:
        return None
    return rows[0]


def _match_lecture(by_full, by_lecture, caption):
    subject = _normalize_text(caption.get("subject"))
    chapter = _normalize_text(caption.get("chapter"))
    lecture = _normalize_text(caption.get("lecture"))

    if subject or chapter or lecture:
        row = _pick_unique(by_full.get((subject, chapter, lecture)))
        if row:
            return row
    if lecture:
        row = _pick_unique(by_lecture.get(lecture))
        if row:
            return row
    return None


def _chunked(items, size=100):
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


async def _delete_messages(client, chat_id, message_ids, dry_run=False):
    if dry_run:
        return
    for chunk in _chunked(message_ids, size=100):
        await client.delete_messages(chat_id, chunk)


def _update_db_for_kept(conn, batch_id, lecture_id, chat_id, message_id, dry_run=False):
    if dry_run:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE lecture_uploads
            SET telegram_chat_id=%s, telegram_message_id=%s
            WHERE batch_id=%s AND lecture_id=%s
            """,
            (str(chat_id), str(message_id), batch_id, lecture_id),
        )
        cur.execute(
            """
            UPDATE lecture_jobs
            SET telegram_chat_id=%s, telegram_message_id=%s
            WHERE batch_id=%s AND lecture_id=%s
            """,
            (str(chat_id), str(message_id), batch_id, lecture_id),
        )
        cur.execute(
            """
            UPDATE backup_id
            SET message_id=%s
            WHERE batch_id=%s AND lecture_id=%s AND platform='telegram' AND channel_id=%s
            """,
            (str(message_id), batch_id, lecture_id, str(chat_id)),
        )
    conn.commit()


def table_exists(conn, table_name):
    with conn.cursor(dictionary=True) as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
            (table_name,),
        )
        row = cur.fetchone()
        return bool(row and row.get("cnt", 0) > 0)


# -------------------- Main --------------------

async def main(chat_id, limit=None, dry_run=False, keep_latest=True):
    conn = connect_db()
    by_full, by_lecture = _build_lecture_maps(conn)

    api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")

    # If user explicitly requested user-session scanning (rare), require API creds
    use_user = os.environ.get("CLEAN_USE_USER_SESSION") == "1"

    if use_user and (not api_id or not api_hash):
        raise RuntimeError("CLEAN_USE_USER_SESSION=1 requires TELEGRAM_API_ID/TELEGRAM_API_HASH")

    if use_user:
        # User session path (requires interactive login or existing session)
        if not bot_token:
            raise RuntimeError("User session mode requires a bot token to also be set for consistency")
        client = TelegramClient('clean_dupes_session', api_id, api_hash)
        await client.start(bot_token=bot_token)

        duplicates = defaultdict(list)
        unmatched = 0

        async for msg in client.iter_messages(chat_id, limit=limit):
            caption = getattr(msg, "message", None) or getattr(msg, "text", None)
            if not caption:
                continue
            parsed = _parse_caption(caption)
            row = _match_lecture(by_full, by_lecture, parsed)
            if not row:
                unmatched += 1
                continue
            lecture_id = str(row.get("lecture_id"))
            batch_id = row.get("batch_id")
            if not lecture_id or not batch_id:
                continue
            key = (str(chat_id), lecture_id)
            duplicates[key].append({
                "message_id": msg.id,
                "batch_id": batch_id,
                "lecture_id": lecture_id,
            })

        # Process duplicates via user session (delete via Telethon)
        for (chat_key, lecture_id), items in duplicates.items():
            if len(items) < 2:
                continue
            items_sorted = sorted(items, key=lambda r: r["message_id"], reverse=keep_latest)
            keep = items_sorted[0]
            drop = items_sorted[1:]
            drop_ids = [r["message_id"] for r in drop]

            print(f"Lecture {lecture_id}: keep {keep['message_id']}, delete {drop_ids}")
            await _delete_messages(client, chat_id, drop_ids, dry_run=dry_run)
            _update_db_for_kept(
                conn,
                keep["batch_id"],
                keep["lecture_id"],
                chat_id,
                keep["message_id"],
                dry_run=dry_run,
            )

        if unmatched:
            print(f"Unmatched messages (no DB mapping): {unmatched}")

        conn.close()
        await client.disconnect()
        return

    # Default: DB-driven cleanup using Bot API for deletions (works with bot token only)
    if not bot_token:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN found; set CLEAN_USE_USER_SESSION=1 to use a user session instead")

    # Aggregate message ids for each (channel, batch_id, lecture_id)
    groups = {}

    with conn.cursor(dictionary=True) as cur:
        # Collect from backup_id (if present)
        if table_exists(conn, "backup_id"):
            cur.execute(
                """
                SELECT batch_id, lecture_id, channel_id AS chat_id, GROUP_CONCAT(message_id ORDER BY created_at DESC SEPARATOR ',') AS msgs
                FROM backup_id
                WHERE platform='telegram' AND channel_id IS NOT NULL AND message_id IS NOT NULL
                GROUP BY batch_id, lecture_id, channel_id
                """
            )
            for r in cur.fetchall():
                key = (str(r['chat_id']), str(r['batch_id']), str(r['lecture_id']))
                msgs = [m for m in (r.get('msgs') or '').split(',') if m]
                groups.setdefault(key, set()).update(msgs)

        # Collect from lecture_uploads (if present)
        if table_exists(conn, "lecture_uploads"):
            cur.execute(
                "SELECT batch_id, lecture_id, telegram_chat_id AS chat_id, telegram_message_id AS msg FROM lecture_uploads WHERE telegram_chat_id IS NOT NULL AND telegram_message_id IS NOT NULL"
            )
            for r in cur.fetchall():
                key = (str(r['chat_id']), str(r['batch_id']), str(r['lecture_id']))
                if r.get('msg'):
                    groups.setdefault(key, set()).add(str(r['msg']))

        # Collect from lecture_jobs (if present)
        if table_exists(conn, "lecture_jobs"):
            cur.execute(
                "SELECT batch_id, lecture_id, telegram_chat_id AS chat_id, telegram_message_id AS msg FROM lecture_jobs WHERE telegram_chat_id IS NOT NULL AND telegram_message_id IS NOT NULL"
            )
            for r in cur.fetchall():
                key = (str(r['chat_id']), str(r['batch_id']), str(r['lecture_id']))
                if r.get('msg'):
                    groups.setdefault(key, set()).add(str(r['msg']))

    to_process = []
    for (chat_id_k, batch_k, lec_k), msgset in groups.items():
        if len(msgset) > 1:
            msg_list = sorted(msgset, key=lambda x: int(x), reverse=keep_latest)
            keep = msg_list[0]
            drop = msg_list[1:]
            to_process.append((chat_id_k, batch_k, lec_k, keep, drop))

    # Delete with Bot API and update DB
    for chat_k, batch_k, lec_k, keep_id, drop_ids in to_process:
        print(f"Lecture {lec_k} in chat {chat_k}: keep {keep_id}, delete {drop_ids}")
        for d in drop_ids:
            if dry_run:
                print(f"  dry-run delete {d}")
                continue
            try:
                resp = requests.post(f"https://api.telegram.org/bot{bot_token}/deleteMessage", data={"chat_id": chat_k, "message_id": int(d)})
                j = resp.json()
                if not j.get('ok'):
                    print(f"  delete failed for {d}: {j}")
            except Exception as e:
                print(f"  delete exception for {d}: {e}")

        # Update DB rows to reference keep_id and remove extra backup entries
        with conn.cursor() as cur:
            if not dry_run:
                if table_exists(conn, "lecture_uploads"):
                    cur.execute(
                        "UPDATE lecture_uploads SET telegram_chat_id=%s, telegram_message_id=%s WHERE batch_id=%s AND lecture_id=%s",
                        (str(chat_k), str(keep_id), batch_k, lec_k),
                    )
                if table_exists(conn, "lecture_jobs"):
                    cur.execute(
                        "UPDATE lecture_jobs SET telegram_chat_id=%s, telegram_message_id=%s WHERE batch_id=%s AND lecture_id=%s",
                        (str(chat_k), str(keep_id), batch_k, lec_k),
                    )
                # delete duplicate backup rows if table exists
                if table_exists(conn, "backup_id"):
                    cur.execute(
                        "DELETE FROM backup_id WHERE platform='telegram' AND batch_id=%s AND lecture_id=%s AND channel_id=%s AND message_id<>%s",
                        (batch_k, lec_k, str(chat_k), str(keep_id)),
                    )
        conn.commit()

    conn.close()


# -------------------- CLI --------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--chat-id", type=int, default=int(os.environ.get("TELEGRAM_CHAT_ID", 0)))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep", choices=["latest", "oldest"], default="latest")
    args = parser.parse_args()

    asyncio.run(
        main(
            chat_id=args.chat_id,
            limit=args.limit,
            dry_run=args.dry_run,
            keep_latest=(args.keep == "latest"),
        )
    )