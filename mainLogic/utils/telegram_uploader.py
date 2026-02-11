import os
import asyncio
import time
import math
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


def _format_eta(seconds_left):
    if seconds_left is None:
        return "--:--"
    seconds_left = int(max(seconds_left, 0))
    minutes, seconds = divmod(seconds_left, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _progress_text(title, pct, speed_bps, eta_seconds, sent_bytes, total_bytes):
    speed_mb = (speed_bps / (1024 * 1024)) if speed_bps and speed_bps > 0 else 0.0
    sent_mb = sent_bytes / (1024 * 1024)
    total_mb = total_bytes / (1024 * 1024) if total_bytes else 0.0
    return (
        f"Uploading: {title}\n"
        f"Progress: {pct:3d}% | {speed_mb:.2f} MB/s | ETA {_format_eta(eta_seconds)} | "
        f"{sent_mb:.1f}/{total_mb:.1f} MB"
    )


async def _upload_async(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False, progress_meta=None):
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
        upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 8)
        part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 1024)
        chat_target = TELEGRAM_CHAT_ID
        if isinstance(chat_target, str):
            stripped = chat_target.strip()
            if stripped.lstrip("-").isdigit():
                chat_target = int(stripped)
        status_msg = None
        upload_title = os.path.basename(file_path)
        if len(upload_title) > 80:
            upload_title = f"...{upload_title[-77:]}"
        last_update_ts = 0.0
        last_pct = -1
        last_bytes = 0
        pm_server = None
        pm_title = upload_title
        if progress_meta and isinstance(progress_meta, dict):
            pm_server = progress_meta.get("server_id")
            pm_title = progress_meta.get("title") or pm_title

        if progress_message:
            try:
                header = (
                    "╭❖━━━━━━━━━━━━━━━━━━━━❖╮\n"
                    "      🎯 LECTURE UPDATE\n"
                    "╰❖━━━━━━━━━━━━━━━━━━━━❖╯\n\n"
                )
                body = (
                    f"File: {pm_title}\n"
                    f"Server: {pm_server or 'N/A'}\n"
                    f"Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n"
                    f"Progress: 0% | 0.00 MB/s | ETA --:-- | 0.0/0.0 MB\n"
                )
                status_msg = await client.send_message(chat_target, header + body)
            except Exception:
                status_msg = None

        def _silence_task(task):
            # Safely consume task exception without raising into the event loop.
            try:
                if task.cancelled():
                    return
                _ = task.exception()
            except asyncio.CancelledError:
                return
            except Exception:
                # Swallow any other errors from edit attempts (FloodWaitError, MessageNotModifiedError, etc.)
                return

        def _progress_wrapper(sent, total):
            nonlocal last_update_ts, last_pct, last_bytes
            if progress_callback:
                progress_callback(sent, total)
            if not progress_message or not status_msg or not total:
                return
            pct = int((sent / total) * 100)
            now = time.monotonic()
            # Throttle edits to avoid Telegram FloodWait; configurable via env var
            try:
                throttle = int(os.environ.get("TELEGRAM_PROGRESS_THROTTLE_SEC", "3"))
            except Exception:
                throttle = 3
            if pct == last_pct and (now - last_update_ts) < throttle:
                return
            elapsed = (now - last_update_ts) if last_update_ts else None
            speed_bps = None
            if elapsed and elapsed > 0:
                speed_bps = (sent - last_bytes) / elapsed
            eta_seconds = None
            if speed_bps and speed_bps > 0:
                eta_seconds = (total - sent) / speed_bps
            last_update_ts = now
            last_pct = pct
            last_bytes = sent
            try:
                # Build a styled progress message
                header = (
                    "╭❖━━━━━━━━━━━━━━━━━━━━❖╮\n"
                    "      🎯 LECTURE UPDATE\n"
                    "╰❖━━━━━━━━━━━━━━━━━━━━❖╯\n\n"
                )
                # Progress bar
                bar_width = 24
                filled = int(round((pct / 100) * bar_width))
                filled = max(0, min(bar_width, filled))
                bar = "█" * filled + "░" * (bar_width - filled)
                speed_mb = (speed_bps / (1024 * 1024)) if speed_bps and speed_bps > 0 else 0.0
                sent_mb = sent / (1024 * 1024)
                total_mb = total / (1024 * 1024) if total else 0.0
                body = (
                    f"File: {pm_title}\n"
                    f"Server: {pm_server or 'N/A'}\n"
                    f"Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n"
                    f"{bar} {pct:3d}%\n"
                    f"{sent_mb:.1f}/{total_mb:.1f} MB | {speed_mb:.2f} MB/s | ETA {_format_eta(eta_seconds)}\n"
                )
                task = asyncio.create_task(client.edit_message(chat_target, status_msg, header + body))
                task.add_done_callback(_silence_task)
            except Exception:
                # Ignore edit failures (MessageNotModified etc.)
                pass
        supports_streaming = as_video and not _get_bool_env("TELEGRAM_DISABLE_STREAMING", False)
        msg = await client.send_file(
            chat_target,
            file_path,
            caption=caption,
            force_document=not as_video,
            supports_streaming=supports_streaming,
            thumb=thumb_path,
            progress_callback=_progress_wrapper,
            workers=upload_workers,
            part_size_kb=part_size_kb,
        )
        if progress_message and status_msg:
            try:
                final = (
                    "╭❖━━━━━━━━━━━━━━━━━━━━❖╮\n"
                    "      🎯 LECTURE UPDATE\n"
                    "╰❖━━━━━━━━━━━━━━━━━━━━❖╯\n\n"
                    f"File: {pm_title}\n"
                    f"Server: {pm_server or 'N/A'}\n"
                    f"Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n"
                    "✅ Upload complete."
                )
                await client.edit_message(chat_target, status_msg, final)
            except Exception:
                pass
        return msg
    finally:
        await client.disconnect()


def upload(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False, progress_meta=None):
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
            progress_meta=progress_meta,
        )
    )
    if isinstance(msg, list) and msg:
        msg = msg[-1]
    chat_id = getattr(getattr(msg, "peer_id", None), "channel_id", None) or getattr(msg, "chat_id", None)
    message_id = getattr(msg, "id", None)
    return {
        "chat_id": chat_id,
        "message_id": message_id,
    }
