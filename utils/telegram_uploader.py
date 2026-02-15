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


def _get_bool_env(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _normalize_chat_id(chat_id):
    if chat_id is None:
        return None
    if isinstance(chat_id, str):
        stripped = chat_id.strip()
        if not stripped:
            return None
        if stripped.lstrip("-").isdigit():
            chat_id = int(stripped)
        else:
            return stripped
    if isinstance(chat_id, int):
        if chat_id < 0:
            return chat_id
        if len(str(chat_id)) >= 10:
            return int(f"-100{chat_id}")
    return chat_id

async def _upload_async(
    file_path,
    caption=None,
    as_video=False,
    progress_callback=None,
    thumb_path=None,
    progress_message=False,
):

    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_API_ID, TELEGRAM_API_HASH]):
        raise RuntimeError(
            "Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_API_ID, TELEGRAM_API_HASH in .env"
        )

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    if thumb_path and not os.path.exists(thumb_path):
        thumb_path = None

    try:
        from pyrogram import Client
        from pyrogram.errors import FloodWait
    except Exception as e:
        raise RuntimeError(f"pyrogram is required: {e}")

    session_name = os.environ.get("TELEGRAM_SESSION_NAME") or f"bot_pyrogram_{os.getpid()}_{int(time.time())}"
    client = Client(
        session_name,
        api_id=int(TELEGRAM_API_ID),
        api_hash=TELEGRAM_API_HASH,
        bot_token=TELEGRAM_BOT_TOKEN,
    )

    await client.start()

    try:
        # 🔥 SPEED TUNING (important part)
        upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 8)      # more parallel workers
        part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 1024)  # bigger chunks

        chat_target = _normalize_chat_id(TELEGRAM_CHAT_ID)

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
            # Throttle edits to avoid Telegram FloodWait; configurable via env var
            try:
                throttle = int(os.environ.get("TELEGRAM_PROGRESS_THROTTLE_SEC", "10"))
            except Exception:
                throttle = 10
            # Only update if percentage changed by at least 5% or time throttle passed
            pct_step = 5
            if abs(pct - last_pct) < pct_step and (now - last_update_ts) < throttle:
                return

            last_update_ts = now
            last_pct = pct
            try:
                asyncio.create_task(
                    client.edit_message(chat_target, status_msg, f"⬆️ Uploading... {pct}%")
                )
            except Exception:
                pass

        if _get_bool_env("TELEGRAM_ALWAYS_DOCUMENT", False):
            as_video = False
        # Use Bot API (via Pyrogram) so we get Bot file_id
        async def _send_with_flood_wait(send_coro):
            while True:
                try:
                    return await send_coro()
                except FloodWait as e:
                    await asyncio.sleep(getattr(e, "value", None) or getattr(e, "x", None) or 10)

        msg = await _send_with_flood_wait(
            lambda: client.send_document(
                chat_target,
                file_path,
                caption=caption,
                thumb=thumb_path,
                progress=_progress_wrapper,
                progress_args=(),
            )
        )

        if progress_message and status_msg:
            try:
                await client.edit_message(chat_target, status_msg, "✅ Upload complete")
            except Exception:
                pass
            # Delete the progress message after a short delay (default 3s)
            try:
                delay = _get_int_env("TELEGRAM_DELETE_PROGRESS_DELAY_SEC", 3) or 3
                await asyncio.sleep(delay)
                try:
                    await client.delete_messages(chat_target, status_msg)
                except Exception:
                    pass
            except Exception:
                # keep silent on any deletion-related errors
                pass

        return msg

    finally:
        await client.stop()


def upload(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False):
    """Upload a file to Telegram using Pyrogram bot.

    Returns a dict with chat_id, message_id, file_id.
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
    chat_id = getattr(getattr(msg, "chat", None), "id", None)
    message_id = getattr(msg, "id", None)
    file_id = msg.document.file_id if getattr(msg, "document", None) else None
    return {
        "chat_id": chat_id,
        "message_id": message_id,
        "file_id": file_id,
    }
