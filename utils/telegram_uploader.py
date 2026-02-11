import os
import asyncio
import time
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH")

def _get_int_env(name, default=None):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


async def _upload_async(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        raise RuntimeError(
            "Telegram credentials not set in environment (.env TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID / TELEGRAM_API_ID / TELEGRAM_API_HASH)"
        )
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    if thumb_path and not os.path.exists(thumb_path):
        thumb_path = None

    try:
        from telethon import TelegramClient
    except Exception as e:
        raise RuntimeError(f"telethon is required: {e}")

    client = TelegramClient("bot_session", int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
    await client.start(bot_token=TELEGRAM_BOT_TOKEN)
    try:
        upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 4)
        part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 512)
        chat_target = TELEGRAM_CHAT_ID
        # Prefer integer IDs for bots; fall back to username strings like @channel
        if isinstance(chat_target, str):
            stripped = chat_target.strip()
            if stripped.lstrip("-").isdigit():
                chat_target = int(stripped)
        status_msg = None
        last_update_ts = 0.0
        last_pct = -1
        if progress_message:
            try:
                status_msg = await client.send_message(chat_target, "⬆️ Uploading... 0%")
            except Exception:
                status_msg = None

        def _progress_wrapper(sent, total):
            nonlocal last_update_ts, last_pct
            if progress_callback:
                progress_callback(sent, total)
            if not progress_message or not status_msg or not total:
                return
            pct = int((sent / total) * 100)
            now = time.monotonic()
            if pct == last_pct and (now - last_update_ts) < 1.5:
                return
            last_update_ts = now
            last_pct = pct
            try:
                asyncio.create_task(
                    client.edit_message(chat_target, status_msg, f"⬆️ Uploading... {pct}%")
                )
            except Exception:
                pass
        msg = await client.send_file(
            chat_target,
            file_path,
            caption=caption,
            force_document=not as_video,
            supports_streaming=as_video,
            thumb=thumb_path,
            progress_callback=_progress_wrapper,
            workers=upload_workers,
            part_size_kb=part_size_kb,
        )
        if progress_message and status_msg:
            try:
                await client.edit_message(chat_target, status_msg, "✅ Upload complete")
            except Exception:
                pass
        return msg
    finally:
        await client.disconnect()


def upload(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False):
    """Upload a file to Telegram using Telethon and bot credentials from environment.

    Returns a dict with chat_id and message_id.
    """
    msg = asyncio.run(
        _upload_async(
            file_path,
            caption=caption,
            as_video=as_video,
            progress_callback=progress_callback,
            thumb_path=thumb_path,
            progress_message=progress_message,
        )
    )
    # telethon can return a Message or a list of Messages
    if isinstance(msg, list) and msg:
        msg = msg[-1]
    chat_id = getattr(getattr(msg, "peer_id", None), "channel_id", None) or getattr(msg, "chat_id", None)
    message_id = getattr(msg, "id", None)
    return {
        "chat_id": chat_id,
        "message_id": message_id,
    }
