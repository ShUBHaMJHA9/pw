#!/usr/bin/env python3
"""
fast.py

High-performance lecture pipeline:
- Async producer/consumer
- Parallel downloads + parallel uploads
- Multiple Telegram sessions for faster MTProto upload
"""

import argparse
import asyncio
import glob
import json
import os
import re
import signal
import time
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, errors as telethon_errors

try:
    from mainLogic.utils import mysql_logger as db_logger
except Exception:
    db_logger = None

try:
    from mainLogic.utils.glv_var import PREFS_FILE
except Exception:
    PREFS_FILE = "preferences.json"

try:
    from beta.batch_scraper_2.Endpoints import Endpoints
except Exception:
    Endpoints = None

from mainLogic.downloader import main as downloader
from mainLogic.utils.gen_utils import generate_safe_folder_name

load_dotenv()


def _log(msg):
    print(msg, flush=True)


def _get_env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return default


def _read_map_json(path):
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        result = {}
        for item in data if isinstance(data, list) else []:
            file_path = item.get("file_path")
            if not file_path:
                continue
            result[os.path.abspath(file_path)] = item
        return result
    except Exception:
        return {}


def _collect_files(files, directory):
    items = []
    if files:
        for f in files:
            p = Path(f)
            if p.exists() and p.is_file():
                items.append(str(p.resolve()))
    if directory:
        d = Path(directory)
        if d.exists() and d.is_dir():
            for p in sorted(d.iterdir()):
                if p.is_file():
                    items.append(str(p.resolve()))
    return items


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


def _get_subject_api_id(subject_obj):
    return (
        getattr(subject_obj, "subjectId", None)
        or getattr(subject_obj, "id", None)
        or getattr(subject_obj, "_id", None)
    )


def _get_chapter_tag_id(chapter_obj):
    return (
        getattr(chapter_obj, "typeId", None)
        or getattr(chapter_obj, "type_id", None)
        or getattr(chapter_obj, "id", None)
        or getattr(chapter_obj, "_id", None)
    )


def _fetch_chapter_lectures(api, batch_id, batch_slug, subject_obj, subject_slug, chapter_obj):
    subject_api_id = _get_subject_api_id(subject_obj)
    tag_ids = []
    primary = _get_chapter_tag_id(chapter_obj)
    if primary:
        tag_ids.append(primary)
    chapter_id = getattr(chapter_obj, "id", None) or getattr(chapter_obj, "_id", None)
    if chapter_id and chapter_id not in tag_ids:
        tag_ids.append(chapter_id)

    if subject_api_id and tag_ids and hasattr(api, "get_batch_chapter_lectures_v3"):
        for tag_id in tag_ids:
            v3 = api.get_batch_chapter_lectures_v3(
                batch_id=batch_id,
                subject_id=subject_api_id,
                tag_id=tag_id,
            )
            if v3:
                return v3
    v2 = api.get_batch_chapters(
        batch_name=batch_slug,
        subject_name=subject_slug,
        chapter_name=getattr(chapter_obj, "name", None) or str(chapter_obj),
    )
    filtered = _filter_lectures_by_tag_name(v2, getattr(chapter_obj, "name", None))
    return filtered


def _build_caption(meta):
    if not meta:
        return None
    parts = []
    if meta.get("course"):
        parts.append(f"Course : {meta.get('course')}")
    if meta.get("subject"):
        parts.append(f"Subject: {meta.get('subject')}")
    if meta.get("chapter"):
        parts.append(f"Chapter: {meta.get('chapter')}")
    if meta.get("lecture"):
        parts.append(f"Lecture: {meta.get('lecture')}")
    if meta.get("teacher"):
        parts.append(f"Teacher: {meta.get('teacher')}")
    if meta.get("start"):
        parts.append(f"Start  : {meta.get('start')}")
    return "\n".join(parts) if parts else None


def _db_call(name, *args, **kwargs):
    if db_logger and hasattr(db_logger, name):
        try:
            return getattr(db_logger, name)(*args, **kwargs)
        except Exception:
            return None
    return None


def _reuse_existing_file(download_dir, safe_name, batch_id=None, lecture_id=None):
    try:
        if db_logger and batch_id and lecture_id:
            recorded = _db_call("get_recorded_file_path", batch_id, lecture_id)
            if recorded and os.path.exists(recorded):
                return recorded
    except Exception:
        pass
    matches = []
    for ext in (".mp4", ".mkv", ".webm", ".mov", ".avi"):
        matches.extend(glob.glob(os.path.join(download_dir, f"{safe_name}*{ext}")))
    if matches:
        return max(matches, key=os.path.getmtime)
    return None


def _download_one(lecture, batch_id, subject_name, chapter_name, download_dir):
    raw_name = "_".join([p for p in [subject_name, chapter_name, getattr(lecture, "name", None)] if p])
    safe_name = generate_safe_folder_name(raw_name)[:200]
    existing = _reuse_existing_file(download_dir, safe_name, batch_id, getattr(lecture, "id", None))
    if existing:
        return existing
    result = downloader(
        id=getattr(lecture, "id", None),
        name=safe_name,
        batch_name=batch_id,
        directory=download_dir,
    )
    if isinstance(result, str) and os.path.exists(result):
        return result
    matches = []
    for ext in (".mp4", ".mkv", ".webm", ".mov", ".avi"):
        matches.extend(glob.glob(os.path.join(download_dir, f"{safe_name}*{ext}")))
    if matches:
        return max(matches, key=os.path.getmtime)
    return None


async def _upload_file(client, chat_id, file_path, caption=None, dry_run=False):
    if dry_run:
        return {"chat_id": chat_id, "message_id": None}
    # Max speed defaults: 512 KB chunks + more parallel workers
    part_size_kb = _get_env_int("TELEGRAM_UPLOAD_PART_SIZE_KB", 512)
    upload_workers = _get_env_int("TELEGRAM_UPLOAD_WORKERS", 32)
    send_timeout = _get_env_int("TELEGRAM_SEND_TIMEOUT_SEC", 600)
    start = time.monotonic()
    msg = await asyncio.wait_for(
        client.send_file(
            chat_id,
            file_path,
            caption=caption,
            part_size_kb=part_size_kb,
            workers=upload_workers,
            supports_streaming=True,
        ),
        timeout=send_timeout,
    )
    elapsed = max(time.monotonic() - start, 0.001)
    try:
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        speed = size_mb / elapsed
        _log(f"Uploaded {os.path.basename(file_path)} at {speed:.2f} MB/s")
    except Exception:
        pass
    return {
        "chat_id": getattr(getattr(msg, "chat", None), "id", None),
        "message_id": getattr(msg, "id", None),
    }


async def _upload_worker(name, queue, chat_id, dry_run=False, delete_after_upload=True, session_prefix="fast_session", stop_event=None):
    api_id = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API_HASH not set")

    session_name = f"{session_prefix}_{name}"
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()

    while True:
        if stop_event and stop_event.is_set():
            break
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        file_path = item.get("file_path")
        meta = item.get("meta") or {}
        try:
            batch_id = meta.get("batch_id")
            lecture_id = meta.get("lecture_id")
            if db_logger and batch_id and lecture_id:
                try:
                    if _db_call("is_upload_done", batch_id, lecture_id):
                        queue.task_done()
                        continue
                except Exception:
                    pass
            caption = meta.get("caption")
            retries = 0
            while True:
                try:
                    result = await _upload_file(client, chat_id, file_path, caption=caption, dry_run=dry_run)
                    break
                except telethon_errors.FloodWaitError as e:
                    wait_s = int(getattr(e, "seconds", 0) or 0)
                    wait_s = max(wait_s, 5)
                    _log(f"[{name}] FloodWait {wait_s}s for {os.path.basename(file_path)}")
                    await asyncio.sleep(wait_s)
                except asyncio.TimeoutError:
                    retries += 1
                    if retries > 3:
                        raise
                    backoff = min(60, 2 ** retries)
                    _log(f"[{name}] timeout, retry {retries} in {backoff}s")
                    await asyncio.sleep(backoff)
                except Exception as exc:
                    retries += 1
                    if retries > 3:
                        raise exc
                    backoff = min(60, 2 ** retries)
                    _log(f"[{name}] retry {retries} in {backoff}s for {os.path.basename(file_path)}")
                    await asyncio.sleep(backoff)

            if db_logger and not dry_run:
                if batch_id and lecture_id:
                    _db_call(
                        "mark_status",
                        batch_id,
                        lecture_id,
                        "done",
                        file_path=file_path,
                        file_size=os.path.getsize(file_path) if file_path and os.path.exists(file_path) else None,
                        telegram_chat_id=str(result.get("chat_id")) if result.get("chat_id") else None,
                        telegram_message_id=str(result.get("message_id")) if result.get("message_id") else None,
                        telegram_file_id=None,
                    )
            if delete_after_upload and file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass
            _log(f"[{name}] uploaded: {os.path.basename(file_path)}")
        except Exception as exc:
            _log(f"[{name}] failed: {file_path} error={exc}")
        finally:
            queue.task_done()

    await client.disconnect()


async def _download_worker(name, queue, upload_queue, download_dir, dry_run=False, stop_event=None):
    while True:
        if stop_event and stop_event.is_set():
            break
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        lecture = item.get("lecture")
        meta = item.get("meta") or {}
        try:
            file_path = None
            if not dry_run:
                file_path = await asyncio.to_thread(
                    _download_one,
                    lecture,
                    meta.get("batch_id"),
                    meta.get("subject"),
                    meta.get("chapter"),
                    download_dir,
                )
            if not file_path and not dry_run:
                raise RuntimeError("download failed")
            if file_path and os.path.exists(file_path) and os.path.getsize(file_path) == 0:
                raise RuntimeError("empty file")
            payload = {
                "file_path": file_path or "",
                "meta": meta,
            }
            await upload_queue.put(payload)
        except Exception as exc:
            _log(f"[download-{name}] failed: {getattr(lecture, 'id', None)} error={exc}")
        finally:
            queue.task_done()


async def _produce(api, batch_slug, batch_id, subjects_filter, chapters_filter, download_queue, max_queue, meta_template, stop_event=None):
    subjects = api.get_batch_details(batch_name=batch_slug)
    tasks = []
    for subj in subjects or []:
        subject_slug = getattr(subj, "slug", None)
        subject_name = getattr(subj, "name", None) or subject_slug
        if not subject_slug:
            continue
        if subjects_filter and subject_slug not in subjects_filter:
            continue
        chapters = api.get_batch_subjects(batch_name=batch_slug, subject_name=subject_slug)
        for chapter in chapters or []:
            chapter_name = getattr(chapter, "name", None)
            if not chapter_name:
                continue
            if chapters_filter and _normalize_tag_name(chapter_name) not in chapters_filter:
                continue
            if getattr(chapter, "videos", 0) == 0:
                continue
            lectures = _fetch_chapter_lectures(api, batch_id, batch_slug, subj, subject_slug, chapter)
            for lecture in lectures or []:
                if stop_event and stop_event.is_set():
                    return
                meta = dict(meta_template)
                meta.update({
                    "batch_id": batch_id,
                    "lecture_id": getattr(lecture, "id", None),
                    "subject": subject_name,
                    "chapter": chapter_name,
                    "lecture": getattr(lecture, "name", None),
                    "start": getattr(lecture, "startTime", None),
                })
                meta["caption"] = _build_caption(meta)
                await download_queue.put({"lecture": lecture, "meta": meta})
                while download_queue.qsize() > max_queue:
                    await asyncio.sleep(0.2)


async def run(args):
    stop_event = asyncio.Event()

    def _signal_handler(_sig, _frame):
        stop_event.set()
        _log("[fast] stop requested, shutting down...")

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    upload_queue = asyncio.Queue(maxsize=args.max_queue)
    download_queue = asyncio.Queue(maxsize=args.max_queue)

    if args.dir or args.files:
        files = _collect_files(args.files, args.dir)
        if not files:
            raise RuntimeError("no files to upload")
        for f in files:
            meta = {
                "batch_id": args.batch_slug,
                "lecture_id": None,
                "subject": args.subjects or "",
                "chapter": args.chapters or "",
                "lecture": os.path.basename(f),
                "caption": None,
            }
            await upload_queue.put({"file_path": f, "meta": meta})
    else:
        api = _init_api()
        if not api:
            raise RuntimeError("API token not found; ensure preferences.json has token_config")
        subjects_filter = set([s.strip() for s in (args.subjects or "").split(",") if s.strip()])
        chapters_filter = set([_normalize_tag_name(s) for s in (args.chapters or "").split(",") if s.strip()])
        meta_template = {
            "course": args.batch_slug,
            "teacher": None,
        }
        producer = asyncio.create_task(
            _produce(api, args.batch_slug, args.batch_slug, subjects_filter, chapters_filter, download_queue, args.max_queue, meta_template, stop_event=stop_event)
        )

    upload_workers = []
    for i in range(args.upload_workers):
        upload_workers.append(
            asyncio.create_task(
                _upload_worker(
                    f"u{i+1}",
                    upload_queue,
                    args.chat_id,
                    dry_run=args.dry_run,
                    delete_after_upload=args.delete_after_upload,
                    session_prefix=args.session_prefix,
                    stop_event=stop_event,
                )
            )
        )

    download_workers = []
    if not args.dir and not args.files:
        for i in range(args.download_workers):
            download_workers.append(
                asyncio.create_task(
                    _download_worker(
                        f"d{i+1}",
                        download_queue,
                        upload_queue,
                        args.download_dir,
                        dry_run=args.dry_run,
                        stop_event=stop_event,
                    )
                )
            )

    if not args.dir and not args.files:
        await producer
        for _ in range(args.download_workers):
            await download_queue.put(None)
        await download_queue.join()

    for _ in range(args.upload_workers):
        await upload_queue.put(None)
    await upload_queue.join()

    for t in download_workers + upload_workers:
        if not t.done():
            t.cancel()


def main():
    parser = argparse.ArgumentParser(description="Fast async download+upload pipeline")
    parser.add_argument("--batch-slug", help="Batch slug", required=False)
    parser.add_argument("--subjects", help="Comma-separated subject slugs", default="")
    parser.add_argument("--chapters", help="Comma-separated chapter names", default="")
    parser.add_argument("--dir", help="Upload all files from directory", default="")
    parser.add_argument("--files", help="Upload specific files (comma-separated)", default="")
    parser.add_argument("--download-dir", help="Download directory", default=".")
    parser.add_argument("--download-workers", type=int, default=3)
    parser.add_argument("--upload-workers", type=int, default=5)
    parser.add_argument("--max-queue", type=int, default=50)
    parser.add_argument("--session-prefix", default="fast_session")
    parser.add_argument("--delete-after-upload", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID"))

    args = parser.parse_args()

    if not args.dir and not args.files and not args.batch_slug:
        raise SystemExit("--batch-slug is required unless --dir/--files is provided")

    if args.files:
        args.files = [s.strip() for s in args.files.split(",") if s.strip()]

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
