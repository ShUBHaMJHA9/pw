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
TELEGRAM_BACKEND = os.environ.get("TELEGRAM_BACKEND", "pyrogram").lower()
TELETHON_SESSION = os.environ.get("TELETHON_SESSION", "telethon_session")


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
    # Preserve non-numeric identifiers (usernames, etc.)
    if isinstance(chat_id, str):
        stripped = chat_id.strip()
        if not stripped:
            return None
        if stripped.lstrip("-").isdigit():
            chat_id = int(stripped)
        else:
            return stripped
    # For numeric IDs, apply -100 prefix for channel IDs when needed
    if isinstance(chat_id, int):
        if chat_id < 0:
            return chat_id
        if len(str(chat_id)) >= 10:
            return int(f"-100{chat_id}")
    return chat_id


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

    # Backend selection: pyrogram (default) or telethon
    if TELEGRAM_BACKEND == 'telethon':
        try:
            from telethon import TelegramClient, errors as telethon_errors
            try:
                import tgcrypto  # optional but speeds up Telethon crypto
            except Exception:
                tgcrypto = None
        except Exception as e:
            raise RuntimeError(f"telethon is required for TELEGRAM_BACKEND=telethon: {e}")

        # Telethon uses user sessions (MTProto). Ensure API_ID/API_HASH set.
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
            raise RuntimeError("TELEGRAM_API_ID / TELEGRAM_API_HASH required for telethon backend")

        client = TelegramClient(TELETHON_SESSION, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
        await client.start()
        if 'tgcrypto' in globals() and globals().get('tgcrypto') is None:
            # warn user that tgcrypto is not installed; it helps performance
            try:
                print("[telegram_uploader] WARNING: tgcrypto not installed — install 'tgcrypto' to improve Telethon upload speed")
            except Exception:
                pass
        try:
            # Telethon expects chat target as int id or username; _normalize_chat_id already handles numeric and strings
            chat_target = _normalize_chat_id(TELEGRAM_CHAT_ID)

            upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 16)
            part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 4096)

            status_msg = None
            upload_title = os.path.basename(file_path)
            if len(upload_title) > 80:
                upload_title = f"...{upload_title[-77:]}"

            if progress_message:
                try:
                    status_msg = await client.send_message(chat_target, f"⬆️ Uploading... 0%\n{upload_title}")
                except Exception:
                    status_msg = None

            last_update_ts = 0.0
            last_pct = -1
            last_bytes = 0

            def _telethon_progress(sent, total):
                nonlocal last_update_ts, last_pct, last_bytes
                if progress_callback:
                    progress_callback(sent, total)
                if not progress_message or not status_msg or not total:
                    return
                pct = int((sent / total) * 100)
                now = time.monotonic()
                try:
                    throttle = int(os.environ.get("TELEGRAM_PROGRESS_THROTTLE_SEC", "10"))
                except Exception:
                    throttle = 10
                pct_step = 5
                if abs(pct - last_pct) < pct_step and (now - last_update_ts) < throttle:
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
                    bar_width = 24
                    filled = int(round((pct / 100) * bar_width))
                    filled = max(0, min(bar_width, filled))
                    bar = "█" * filled + "░" * (bar_width - filled)
                    speed_mb = (speed_bps / (1024 * 1024)) if speed_bps and speed_bps > 0 else 0.0
                    sent_mb = sent / (1024 * 1024)
                    total_mb = total / (1024 * 1024) if total else 0.0
                    body = (
                        f"File: {upload_title}\n"
                        f"Server: {None or 'N/A'}\n"
                        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n"
                        f"{bar} {pct:3d}%\n"
                        f"{sent_mb:.1f}/{total_mb:.1f} MB | {speed_mb:.2f} MB/s | ETA {_format_eta(eta_seconds)}\n"
                    )
                    asyncio.create_task(client.edit_message(status_msg, body))
                except Exception:
                    pass

            # Send file via Telethon (user session). Telethon will handle chunking.
            try:
                msg = await client.send_file(
                    chat_target,
                    file_path,
                    caption=caption,
                    thumb=thumb_path,
                    progress_callback=_telethon_progress,
                    part_size_kb=part_size_kb,
                )
            except telethon_errors.FloodWait as e:
                await asyncio.sleep(getattr(e, 'seconds', 10) or 10)
                msg = await client.send_file(chat_target, file_path, caption=caption)

            # Telethon message may contain media info
            file_id = None
            file_type = None
            if msg and getattr(msg, 'media', None):
                file_type = 'document'  # best-effort

            if progress_message and status_msg:
                try:
                    await client.edit_message(status_msg, f"✅ Upload complete\n{upload_title}")
                except Exception:
                    pass

            return msg, file_id, file_type
        finally:
            await client.disconnect()

    # Default: pyrogram bot backend
    try:
        from pyrogram import Client
        from pyrogram.errors import FloodWait
    except Exception as e:
        raise RuntimeError(f"pyrogram is required: {e}")

    client = Client(
        "bot_pyrogram",
        api_id=int(TELEGRAM_API_ID),
        api_hash=TELEGRAM_API_HASH,
        bot_token=TELEGRAM_BOT_TOKEN,
    )
    await client.start()
    try:
        upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 16)
        part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 4096)
        chat_target = _normalize_chat_id(TELEGRAM_CHAT_ID)
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
                throttle = int(os.environ.get("TELEGRAM_PROGRESS_THROTTLE_SEC", "10"))
            except Exception:
                throttle = 10
            # Only update if percentage changed by at least 5% or time throttle passed
            pct_step = 5
            if abs(pct - last_pct) < pct_step and (now - last_update_ts) < throttle:
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
                asyncio.create_task(
                    client.edit_message_text(chat_target, status_msg.id, header + body)
                )
            except Exception:
                # Ignore edit failures (MessageNotModified etc.)
                pass
        async def _send_with_flood_wait(send_coro):
            while True:
                try:
                    return await send_coro()
                except FloodWait as e:
                    await asyncio.sleep(getattr(e, "value", None) or getattr(e, "x", None) or 10)
        if _get_bool_env("TELEGRAM_ALWAYS_DOCUMENT", False):
            as_video = False
        supports_streaming = as_video and not _get_bool_env("TELEGRAM_DISABLE_STREAMING", False)
        # Some Pyrogram versions expose `send_file`; if not, fall back to send_video/send_document
        send_file_fn = getattr(client, 'send_file', None)
        if send_file_fn:
            if as_video:
                msg = await _send_with_flood_wait(
                    lambda: client.send_file(
                        chat_target,
                        file_path,
                        video=True,
                        caption=caption,
                        thumb=thumb_path,
                        supports_streaming=supports_streaming,
                        progress=_progress_wrapper,
                        progress_args=(),
                        part_size_kb=part_size_kb,
                        workers=upload_workers,
                    )
                )
                file_id = getattr(getattr(msg, 'video', None), 'file_id', None) or getattr(getattr(msg, 'media', None), 'document', None)
                file_type = "video"
            else:
                msg = await _send_with_flood_wait(
                    lambda: client.send_file(
                        chat_target,
                        file_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=_progress_wrapper,
                        progress_args=(),
                        part_size_kb=part_size_kb,
                        workers=upload_workers,
                    )
                )
                file_id = getattr(getattr(msg, 'document', None), 'file_id', None) or getattr(getattr(msg, 'media', None), 'document', None)
                file_type = "document"
        else:
            # Older Pyrogram: no send_file; use send_video/send_document without part_size/workers
            if as_video:
                msg = await _send_with_flood_wait(
                    lambda: client.send_video(
                        chat_target,
                        file_path,
                        caption=caption,
                        thumb=thumb_path,
                        supports_streaming=supports_streaming,
                        progress=_progress_wrapper,
                        progress_args=(),
                    )
                )
                file_id = getattr(getattr(msg, 'video', None), 'file_id', None)
                file_type = "video"
            else:
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
                file_id = getattr(getattr(msg, 'document', None), 'file_id', None)
                file_type = "document"
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
                await client.edit_message_text(chat_target, status_msg.id, final)
            except Exception:
                pass
            # Delete the progress message after a short delay (default 3s)
            try:
                try:
                    delay = int(os.environ.get("TELEGRAM_DELETE_PROGRESS_DELAY_SEC", "3"))
                except Exception:
                    delay = 3
                await asyncio.sleep(delay)
                try:
                    await client.delete_messages(chat_target, status_msg)
                except Exception:
                    pass
            except Exception:
                pass
        return msg, file_id, file_type
    finally:
        await client.stop()


def upload(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False, progress_meta=None):
    """Upload a file to Telegram using Pyrogram bot.

    Returns a dict with chat_id, message_id, file_id, and file_type.
    """
    msg, file_id, file_type = asyncio.run(
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
    chat_id = getattr(getattr(msg, "chat", None), "id", None)
    message_id = getattr(msg, "id", None)
    return {
        "chat_id": chat_id,
        "message_id": message_id,
        "file_id": file_id,
        "file_type": file_type,
    }
