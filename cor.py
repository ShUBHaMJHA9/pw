#!/usr/bin/env python3
"""
cor.py

Backfill Bot API `file_id` into `telegram_file_id` by:
- uploading a local file if available
- or downloading from Telegram via Telethon and reuploading with Bot API

Usage:
    python cor.py [--dry-run] [--limit N] [--batch BATCH_ID]

Requirements:
- TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN
- PWDL_DB_URL (MySQL)
"""

import os
import asyncio
import requests
import time
import tempfile
from dotenv import load_dotenv
from telethon import TelegramClient
from mainLogic.utils import mysql_logger as db

load_dotenv()


def _has_column(cur, table, column):
    cur.execute(
        """
        SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    return cur.fetchone().get("cnt", 0) > 0


def _normalize_chat_id(chat_id):
    if chat_id is None:
        return None
    try:
        raw = int(str(chat_id).strip())
    except Exception:
        return chat_id
    # If it's already a channel id with -100 prefix, keep it
    if raw < 0:
        return raw
    # If looks like a channel id (large positive), add -100 prefix
    if len(str(raw)) >= 10:
        return int(f"-100{raw}")
    return raw


def fetch_rows(conn, limit=None, batch_filter=None):
    with conn.cursor() as cur:
        has_file_col = _has_column(cur, "lecture_uploads", "telegram_file_id")
        sql = """
        SELECT lu.batch_id, lu.lecture_id, lu.telegram_chat_id, lu.telegram_message_id,
               COALESCE(lu.file_path, lj.file_path) AS file_path
        FROM lecture_uploads lu
        LEFT JOIN lecture_jobs lj 
          ON lj.batch_id = lu.batch_id AND lj.lecture_id = lu.lecture_id
        WHERE lu.telegram_chat_id IS NOT NULL 
                    AND lu.telegram_message_id IS NOT NULL
        """
        if has_file_col:
            sql += " AND (lu.telegram_file_id IS NULL OR lu.telegram_file_id = '')"
        params = []
        if batch_filter:
            sql += " AND lu.batch_id = %s"
            params.append(batch_filter)
        sql += " ORDER BY lu.created_at DESC"
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def update_bot_file_id(conn, batch_id, lecture_id, file_id):
    with conn.cursor() as cur:
        # Add column if missing
        if not _has_column(cur, "lecture_uploads", "telegram_file_id"):
            cur.execute("ALTER TABLE lecture_uploads ADD COLUMN telegram_file_id VARCHAR(255) NULL")
            print("Added column lecture_uploads.telegram_file_id")
        if not _has_column(cur, "lecture_jobs", "telegram_file_id"):
            cur.execute("ALTER TABLE lecture_jobs ADD COLUMN telegram_file_id VARCHAR(255) NULL")
            print("Added column lecture_jobs.telegram_file_id")

        # Update DB
        cur.execute("""
            UPDATE lecture_uploads SET telegram_file_id=%s WHERE batch_id=%s AND lecture_id=%s
        """, (file_id, batch_id, lecture_id))
        cur.execute("""
            UPDATE lecture_jobs SET telegram_file_id=%s WHERE batch_id=%s AND lecture_id=%s
        """, (file_id, batch_id, lecture_id))
    conn.commit()


def bot_upload_file(chat_id, file_path, timeout_sec=300):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        return None
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            resp = requests.post(url, data={"chat_id": chat_id}, files={"document": f}, timeout=timeout_sec)
        data = resp.json()
        if data.get("ok"):
            return data["result"]["document"]["file_id"]
        print(f"  -> bot upload response not ok: {data}")
    except Exception as e:
        print(f"Bot upload failed: {e}")
    return None


async def main(limit=None, batch=None, dry_run=False, download_timeout=600, upload_timeout=300, allow_download=True):
    db.init(None)
    conn = db._connect()

    rows = fetch_rows(conn, limit=limit, batch_filter=batch)
    if not rows:
        print("No uploads missing bot_file_id")
        return

    api_id = int(os.environ.get("TELEGRAM_API_ID", 0))
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    client = TelegramClient("cor_botfile_session", api_id, api_hash)
    await client.start(bot_token=bot_token)

    try:
        for i, row in enumerate(rows, start=1):
            batch_id = row["batch_id"]
            lecture_id = row["lecture_id"]
            chat = _normalize_chat_id(row["telegram_chat_id"])
            msg_id = int(row["telegram_message_id"])
            file_path = row.get("file_path")

            print(f"[{i}/{len(rows)}] {batch_id}/{lecture_id} -> chat={chat} msg={msg_id}")

            bot_file_id = None

            # 1) Use local file if exists
            if file_path and os.path.exists(file_path):
                print("  -> uploading local file with Bot API...")
                bot_file_id = bot_upload_file(chat, file_path, timeout_sec=upload_timeout)
                if bot_file_id:
                    print("  -> Bot API file_id captured via local file")

            # 2) Fallback: download via Telethon + upload to bot (optional)
            if not bot_file_id and allow_download:
                print("  -> downloading media via Telethon...")
                try:
                    msg = await client.get_messages(chat, ids=msg_id)
                except ValueError:
                    # Try Telethon entity lookup fallback for channels/users
                    entity = await client.get_entity(chat)
                    msg = await client.get_messages(entity, ids=msg_id)
                if not msg or not msg.media:
                    print("  -> message has no media, skipping")
                    continue

                # Download to temp file
                tmp_handle = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
                tmp_path = tmp_handle.name
                tmp_handle.close()
                start = time.time()
                try:
                    await asyncio.wait_for(client.download_media(msg.media, tmp_path), timeout=download_timeout)
                except asyncio.TimeoutError:
                    print(f"  -> Telethon download timed out after {download_timeout}s")
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                    continue
                elapsed = time.time() - start
                print(f"  -> download complete in {elapsed:.1f}s, uploading with Bot API...")
                bot_file_id = bot_upload_file(chat, tmp_path, timeout_sec=upload_timeout)
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                if bot_file_id:
                    print("  -> Bot API file_id captured via Telethon download + bot upload")

            if bot_file_id and not dry_run:
                update_bot_file_id(conn, batch_id, lecture_id, bot_file_id)
                print("  -> DB updated")
            elif not bot_file_id:
                if allow_download:
                    print("  -> failed to get Bot API file_id")
                else:
                    print("  -> no local file; download disabled, skipping")

    finally:
        await client.disconnect()
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--batch", type=str)
    parser.add_argument("--download-timeout", type=int, default=600)
    parser.add_argument("--upload-timeout", type=int, default=300)
    parser.add_argument("--no-download", action="store_true", help="Skip Telethon download fallback; use local files only")
    args = parser.parse_args()

    asyncio.run(
        main(
            limit=args.limit,
            batch=args.batch,
            dry_run=args.dry_run,
            download_timeout=args.download_timeout,
            upload_timeout=args.upload_timeout,
            allow_download=not args.no_download,
        )
    )
