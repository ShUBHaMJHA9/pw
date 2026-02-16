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


def _sleep_with_backoff(attempt, base=3.0, cap=60.0):
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    return delay


def _extract_floodwait_seconds(exc, default=10):
    # Try common attributes used by different clients
    for attr in ("seconds", "x", "value", "wait", "wait_seconds"):
        val = getattr(exc, attr, None)
        if val is not None:
            try:
                return int(val)
            except Exception:
                continue
    # Fallback: try to parse integer from exception string
    try:
        import re

        m = re.search(r"(\d{1,6})", str(exc))
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return int(default)


async def mtproto_batch_upload(file_paths, chat_id=None, session_name=None, concurrency=2, caption=None, thumb_path=None, as_video=False, progress_callback=None):
    """Batch upload files via Telethon (MTProto) using a single user session.

    - Uses streaming uploads (no full file in memory)
    - Handles FloodWait with sleep
    - Exponential backoff retry (max 3)
    """
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        raise RuntimeError("TELEGRAM_API_ID / TELEGRAM_API_HASH required for Telethon uploads")
    if not file_paths:
        return []

    chat_target = _normalize_chat_id(chat_id or TELEGRAM_CHAT_ID)
    if not chat_target:
        raise RuntimeError("TELEGRAM_CHAT_ID required")

    from telethon import TelegramClient, errors as telethon_errors

    part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 16384)
    max_retries = _get_int_env("TELEGRAM_UPLOAD_MAX_RETRIES", 3)
    # Use a higher default for workers to speed up large file uploads on capable networks
    upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 64)

    session = session_name or TELETHON_SESSION
    client = TelegramClient(session, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
    # Start client with FloodWait handling to avoid auth.ImportBotAuthorization throttling
    start_attempts = 0
    while True:
        try:
            await client.start()
            break
        except Exception as e:
            # Telethon exposes FloodWaitError
            if isinstance(e, getattr(telethon_errors, 'FloodWaitError', ())) or 'FLOOD_WAIT' in str(e) or 'auth.ImportBotAuthorization' in str(e):
                start_attempts += 1
                wait_for = _extract_floodwait_seconds(e, default=10)
                await asyncio.sleep(wait_for)
                if start_attempts >= 3:
                    raise
                continue
            raise

    results = []
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _upload_one(path):
        if not os.path.exists(path):
            return {"file_path": path, "ok": False, "error": "not_found"}
        attempts = 0
        while attempts < max_retries:
            attempts += 1
            try:
                # Determine send timeout: allow explicit opt-out (no timeout).
                try:
                    raw = os.environ.get("TELEGRAM_SEND_TIMEOUT_SEC", "")
                    rl = str(raw).strip().lower() if raw is not None else ""
                    if rl in ("0", "none", "inf", "infinite", "-1", "unlimited"):
                        send_timeout = None
                    elif rl == "":
                        # Default: no timeout to avoid failures on very large uploads
                        send_timeout = None
                    else:
                        send_timeout = int(raw)
                except Exception:
                    send_timeout = None
                start = time.monotonic()
                print(f"[mtproto] sending {os.path.basename(path)} timeout={send_timeout}s attempt={attempts}")
                # Pass caption/thumb/video flags through to Telethon send_file
                send_kwargs = {
                    'part_size_kb': part_size_kb,
                    'workers': max(1, int(upload_workers or 1)),
                }
                if caption:
                    send_kwargs['caption'] = caption
                if thumb_path:
                    send_kwargs['thumb'] = thumb_path
                if as_video:
                    send_kwargs['video'] = True
                # Forward progress callback if provided so callers receive progress updates
                if progress_callback:
                    send_kwargs['progress_callback'] = progress_callback
                if send_timeout is None:
                    msg = await client.send_file(chat_target, path, **send_kwargs)
                else:
                    msg = await asyncio.wait_for(
                        client.send_file(chat_target, path, **send_kwargs), timeout=send_timeout
                    )
                elapsed = max(time.monotonic() - start, 0.001)
                size_mb = os.path.getsize(path) / (1024 * 1024)
                speed = size_mb / elapsed
                print(f"[mtproto] {os.path.basename(path)} {speed:.2f} MB/s")
                return {
                    "file_path": path,
                    "ok": True,
                    "chat_id": getattr(getattr(msg, "chat", None), "id", None),
                    "message_id": getattr(msg, "id", None),
                }
            except asyncio.TimeoutError:
                print(f"[mtproto] send_file timed out after {send_timeout}s (attempt {attempts})")
                backoff = _sleep_with_backoff(attempts)
                await asyncio.sleep(backoff)
                last_error = f"timeout {send_timeout}s"
            except telethon_errors.FloodWaitError as e:
                wait_for = int(getattr(e, "seconds", 10) or 10)
                await asyncio.sleep(wait_for)
            except Exception as e:
                backoff = _sleep_with_backoff(attempts)
                await asyncio.sleep(backoff)
                last_error = str(e)
        return {"file_path": path, "ok": False, "error": last_error}

    async def _task(path):
        async with sem:
            return await _upload_one(path)

    tasks = [asyncio.create_task(_task(p)) for p in file_paths]
    for done in asyncio.as_completed(tasks):
        results.append(await done)

    await client.disconnect()
    return results


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
        # Start client with FloodWait handling
        start_attempts = 0
        while True:
            try:
                await client.start()
                break
            except Exception as e:
                if isinstance(e, getattr(telethon_errors, 'FloodWaitError', ())) or 'FLOOD_WAIT' in str(e) or 'auth.ImportBotAuthorization' in str(e):
                    start_attempts += 1
                    wait_for = _extract_floodwait_seconds(e, default=10)
                    await asyncio.sleep(wait_for)
                    if start_attempts >= 3:
                        raise
                    continue
                raise
        if 'tgcrypto' in globals() and globals().get('tgcrypto') is None:
            # warn user that tgcrypto is not installed; it helps performance
            try:
                print("[telegram_uploader] WARNING: tgcrypto not installed — install 'tgcrypto' to improve Telethon upload speed")
            except Exception:
                pass
        try:
            # Telethon expects chat target as int id or username; _normalize_chat_id already handles numeric and strings
            chat_target = _normalize_chat_id(TELEGRAM_CHAT_ID)

            # Pick tunable defaults; allow env override
            upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 64)
            part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 8192)
            # Auto-increase part size for very large files to reduce chunk count
            try:
                _size = os.path.getsize(file_path) if os.path.exists(file_path) else None
                _size_mb = (_size / (1024 * 1024)) if _size else 0
            except Exception:
                _size_mb = 0
            if _size_mb > 1024:
                part_size_kb = max(part_size_kb, 65536)  # 64 MB
            elif _size_mb > 512:
                part_size_kb = max(part_size_kb, 32768)  # 32 MB
            elif _size_mb > 200:
                part_size_kb = max(part_size_kb, 16384)  # 16 MB

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
                # Use workers for parallel chunk uploads to improve speed
                msg = await client.send_file(
                    chat_target,
                    file_path,
                    caption=caption,
                    thumb=thumb_path,
                    progress_callback=_telethon_progress,
                    part_size_kb=part_size_kb,
                    workers=max(1, int(upload_workers or 1)),
                )
            except Exception as e:
                if isinstance(e, getattr(telethon_errors, 'FloodWaitError', ())) or 'FLOOD_WAIT' in str(e):
                    wait_for = _extract_floodwait_seconds(e, default=10)
                    await asyncio.sleep(wait_for)
                    msg = await client.send_file(chat_target, file_path, caption=caption, workers=max(1, int(upload_workers or 1)))
                else:
                    raise

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

    # Optional: TDLib / tdlight backend (very fast, native TDLib engine)
    if TELEGRAM_BACKEND in ("tdlib", "tdlight"):
        # Provide a defensive skeleton for TDLib-based uploaders. Actual
        # integration depends on the specific Python bindings you install
        # (e.g. python-tdlib, tdlight-py, pytglib). This code tries common
        # import names and calls a minimal async interface if present.
        try:
            # Prefer tdlight wrapper if present
            from tdlight import TDLightClient as TDClient  # type: ignore
        except Exception:
            try:
                from tdlib import TDLib as TDClient  # type: ignore
            except Exception:
                raise RuntimeError(
                    "TDLib backend selected but no tdlib/tdlight Python bindings found. "
                    "Install 'python-tdlib' or 'tdlight' and retry."
                )

        client = None
        try:
            # Instantiate client (bindings vary; best-effort call)
            try:
                client = TDClient()
            except TypeError:
                # Some wrappers require (session, api_id, api_hash)
                try:
                    client = TDClient(TELETHON_SESSION, int(TELEGRAM_API_ID or 0), TELEGRAM_API_HASH)
                except Exception:
                    client = TDClient()

            # Start client; allow bot token if supported by binding
            start_attempts = 0
            while True:
                try:
                    if TELEGRAM_BOT_TOKEN and hasattr(client, 'start'):
                        await client.start(bot_token=TELEGRAM_BOT_TOKEN)
                    elif hasattr(client, 'start'):
                        await client.start()
                    break
                except Exception as e:
                    # Use FloodWait helper and cap retries
                    if 'FLOOD_WAIT' in str(e) or 'auth.ImportBotAuthorization' in str(e):
                        start_attempts += 1
                        wait_for = _extract_floodwait_seconds(e, default=10)
                        await asyncio.sleep(wait_for)
                        if start_attempts >= 3:
                            raise
                        continue
                    raise

            send_fn = getattr(client, 'send_file', None) or getattr(client, 'sendFile', None)
            if not send_fn:
                raise RuntimeError("TDLib binding does not expose a compatible send_file API")

            try:
                msg = await send_fn(chat_target, file_path, caption=caption, progress_callback=_progress_wrapper)
            except Exception as e:
                if 'FLOOD_WAIT' in str(e):
                    wait_for = _extract_floodwait_seconds(e, default=10)
                    await asyncio.sleep(wait_for)
                    msg = await send_fn(chat_target, file_path, caption=caption)
                else:
                    raise

            # TDLib may not provide the same message object shape; return best-effort
            file_id = None
            file_type = None
            try:
                if getattr(msg, 'content', None):
                    file_type = 'document'
            except Exception:
                pass

            return msg, file_id, file_type
        finally:
            try:
                if client is not None and hasattr(client, 'stop'):
                    await client.stop()
            except Exception:
                pass

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
    # Start pyrogram client with FloodWait handling
    start_attempts = 0
    while True:
        try:
            await client.start()
            break
        except Exception as e:
            try:
                from pyrogram.errors import FloodWait as _PyroFlood
            except Exception:
                _PyroFlood = None
            if (_PyroFlood and isinstance(e, _PyroFlood)) or 'FLOOD_WAIT' in str(e) or 'auth.ImportBotAuthorization' in str(e):
                start_attempts += 1
                wait_for = _extract_floodwait_seconds(e, default=10)
                await asyncio.sleep(wait_for)
                if start_attempts >= 3:
                    raise
                continue
            raise
    try:
        # Pyrogram: tune defaults for faster transfers; can be overridden via env
        upload_workers = _get_int_env("TELEGRAM_UPLOAD_WORKERS", 64)
        part_size_kb = _get_int_env("TELEGRAM_UPLOAD_PART_SIZE_KB", 8192)
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
        async def _send_with_flood_wait(send_coro, max_attempts=3):
            attempts = 0
            # Per-attempt timeout (seconds)
            try:
                raw = os.environ.get("TELEGRAM_SEND_TIMEOUT_SEC", "")
                if raw is None:
                    raw = ""
                rl = str(raw).strip().lower()
                # Explicit opt-out values indicate no timeout (wait until upload completes)
                if rl in ("0", "none", "inf", "infinite", "-1", "unlimited"):
                    send_timeout = None
                elif rl == "":
                    # Default: no timeout to avoid failures on very large uploads
                    send_timeout = None
                else:
                    send_timeout = int(raw)
            except Exception:
                send_timeout = 600
            while True:
                try:
                    # If send_timeout is None, don't enforce an upper bound — allow upload to complete
                    if send_timeout is None:
                        return await send_coro()
                    # Use wait_for to abort truly stuck uploads
                    return await asyncio.wait_for(send_coro(), timeout=send_timeout)
                except asyncio.TimeoutError:
                    attempts += 1
                    print(f"[telegram_uploader] send_file timed out after {send_timeout}s (attempt {attempts})")
                    if attempts >= max_attempts:
                        raise
                    # short backoff before retry
                    await asyncio.sleep(_sleep_with_backoff(attempts, base=3.0, cap=60.0))
                    continue
                except Exception as e:
                    attempts += 1
                    # FloodWait from Pyrogram has .value or .x, but be permissive
                    if isinstance(e, FloodWait) or 'FLOOD_WAIT' in str(e) or 'auth.ImportBotAuthorization' in str(e):
                        wait_for = _extract_floodwait_seconds(e, default=10)
                        await asyncio.sleep(wait_for)
                        if attempts >= max_attempts:
                            raise
                        continue
                    raise
        if _get_bool_env("TELEGRAM_ALWAYS_DOCUMENT", False):
            as_video = False
        supports_streaming = as_video and not _get_bool_env("TELEGRAM_DISABLE_STREAMING", False)
        # Some Pyrogram versions expose `send_file`; if not, fall back to send_video/send_document
        send_file_fn = getattr(client, 'send_file', None)
        if send_file_fn:
            if as_video:
                try:
                    size = os.path.getsize(file_path) if os.path.exists(file_path) else None
                except Exception:
                    size = None
                print(f"[telegram_uploader] Starting pyrogram send_file (video) -> {chat_target} size={size} workers={upload_workers} part_kb={part_size_kb}")
                try:
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
                except Exception as e:
                    print(f"[telegram_uploader] send_file(video) failed: {e}")
                    raise
                file_id = getattr(getattr(msg, 'video', None), 'file_id', None) or getattr(getattr(msg, 'media', None), 'document', None)
                file_type = "video"
            else:
                try:
                    size = os.path.getsize(file_path) if os.path.exists(file_path) else None
                except Exception:
                    size = None
                print(f"[telegram_uploader] Starting pyrogram send_file (doc) -> {chat_target} size={size} workers={upload_workers} part_kb={part_size_kb}")
                try:
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
                except Exception as e:
                    print(f"[telegram_uploader] send_file(document) failed: {e}")
                    raise
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
