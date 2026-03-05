#!/usr/bin/env python3
"""Interactive Khazana downloader.

This script walks Khazana program -> subject -> teacher -> topic -> sub-topic
and downloads selected lectures. It records status in the
`khazana_lecture_uploads` table when DB logging is enabled.
"""
import json
import os
import re
import socket
import sys
import time
from urllib.parse import urlparse

import requests

from beta.batch_scraper_2.module import ScraperModule
from beta.batch_scraper_2.Endpoints import Endpoints
from mainLogic.main import Main
from mainLogic.utils.Endpoint import Endpoint
from mainLogic.utils.glv_var import PREFS_FILE, debugger

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# Add bin directory to PATH for mp4decrypt
bin_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")
if os.path.exists(bin_dir) and bin_dir not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"

prefs = ScraperModule.prefs


def _start_task_tui(task_title):
    try:
        from tui import DownloaderTUI
    except Exception:
        return None, None

    tui = DownloaderTUI(verbose=True)
    sink = None

    try:
        tui.start()
        tui.set_status(task_title)

        def _sink(level="INFO", message="", formatted=""):
            msg_level = level if level in {"INFO", "DEBUG", "WARNING", "ERROR"} else "INFO"
            tui.log(str(message), msg_level)

        sink = _sink
        debugger.add_log_sink(sink)
    except Exception:
        try:
            if sink:
                debugger.remove_log_sink(sink)
        except Exception:
            pass
        try:
            tui.stop()
        except Exception:
            pass
        return None, None

    return tui, sink


def _stop_task_tui(tui, sink):
    if sink:
        try:
            debugger.remove_log_sink(sink)
        except Exception:
            pass
    if tui:
        try:
            tui.stop()
        except Exception:
            pass


def _select_user_and_init_api(prefs):
    users = prefs.get("users", []) if isinstance(prefs, dict) else []
    if not users:
        return ScraperModule.batch_api, None

    print("Multiple user profiles found. Select user to use for API requests:")
    for idx, u in enumerate(users, start=1):
        uname = u.get("name") or u.get("username") or f"user-{idx}"
        token_preview = (u.get("access_token") or u.get("token") or "")[:8]
        print(f"  {idx}. {uname} (token startswith: {token_preview}...)")
    print("  a. Add new user")
    print("  q. Quit")
    sel = input("Choose user (number) or action [1]: ").strip()
    if not sel:
        sel = "1"
    if sel.lower() == "q":
        print("Exiting.")
        sys.exit(0)
    if sel.lower() == "a":
        name = input("Enter profile name: ").strip() or f"user-{len(users)+1}"
        token = input("Enter access token (Bearer token string): ").strip()
        random_id = input("Enter random_id (optional): ").strip() or None
        new_user = {"name": name, "access_token": token}
        if random_id:
            new_user["random_id"] = random_id
        users.append(new_user)
        try:
            with open(PREFS_FILE, "r", encoding="utf-8") as handle:
                pf = json.load(handle)
        except Exception:
            pf = prefs if isinstance(prefs, dict) else {}
        pf["users"] = users
        try:
            with open(PREFS_FILE, "w", encoding="utf-8") as handle:
                json.dump(pf, handle, indent=2)
            print(f"Saved new profile to preferences: {PREFS_FILE}")
        except Exception as e:
            debugger.error(f"Failed to save preferences file: {e}")
        chosen = users[-1]
    else:
        try:
            idx = int(sel) - 1
            if idx < 0 or idx >= len(users):
                print("Invalid selection, defaulting to first user.")
                idx = 0
            chosen = users[idx]
        except ValueError:
            print("Invalid input, defaulting to first user.")
            chosen = users[0]

    token = chosen.get("access_token") or chosen.get("token") or chosen.get("token_config", {}).get("access_token")
    random_id = chosen.get("random_id") or chosen.get("randomId") or chosen.get("token_config", {}).get("random_id")
    if not token:
        debugger.error("Selected profile does not have a token. Please update preferences.")
        return ScraperModule.batch_api, None

    try:
        if random_id:
            return Endpoints(verbose=False).set_token(token, random_id=random_id), random_id
        return Endpoints(verbose=False).set_token(token), random_id
    except Exception as e:
        debugger.error(f"Failed to initialize API with selected profile: {e}")
        return ScraperModule.batch_api, random_id


def _pick_from_list(items, title, get_label):
    if not items:
        return []
    print(title)
    for idx, item in enumerate(items, start=1):
        print(f"  {idx}. {get_label(item)}")
    choice = input("Enter number(s) comma-separated, or 'all' to pick all: ").strip()
    if not choice or choice.lower() == "all":
        return items
    selected = []
    for part in choice.split(","):
        try:
            i = int(part.strip()) - 1
            if 0 <= i < len(items):
                selected.append(items[i])
        except ValueError:
            continue
    return selected


def _get_val(obj, key):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _looks_like_object_id(value):
    if not value:
        return False
    text = str(value).strip()
    return len(text) == 24 and all(ch in "0123456789abcdefABCDEF" for ch in text)


def _resolve_khazana_program_id(batch_api, batch_slug_or_id):
    if not batch_slug_or_id:
        return None
    if _looks_like_object_id(batch_slug_or_id):
        return batch_slug_or_id

    url = f"https://api.penpencil.co/v3/batches/{batch_slug_or_id}/details"
    try:
        endpoint = Endpoint(url, headers=getattr(batch_api, "DEFAULT_HEADERS", None))
        data, status_code, _ = endpoint.fetch()
        if status_code != 200 or not isinstance(data, dict):
            return None
        details = data.get("data") or {}
        program_id = details.get("khazanaProgramId")
        if program_id:
            return str(program_id)
    except Exception:
        return None
    return None


def _list_khazana_packs(batch_api):
    packs = []
    try:
        batches = batch_api.get_purchased_batches(all_pages=True)
    except Exception:
        batches = []
    for batch in batches or []:
        slug = batch.get("slug") or batch.get("_id")
        name = batch.get("name") or slug
        if not slug:
            continue
        program_id = _resolve_khazana_program_id(batch_api, slug)
        if program_id:
            packs.append({
                "name": name,
                "slug": slug,
                "program_id": program_id,
            })
    return packs


def _get_display_name(obj):
    for key in ("name", "title", "slug", "id", "_id"):
        value = _get_val(obj, key)
        if value:
            return str(value)
    return str(obj)


def _get_teacher_label(obj):
    """Build a readable teacher label from Khazana chapter payload."""
    base = _get_display_name(obj)
    if not isinstance(obj, dict):
        return base

    description = _get_val(obj, "description")
    teacher_from_desc = None
    if isinstance(description, str) and description.strip():
        # API usually returns: "Pankaj Sharma Sir ;Hinglish"
        teacher_from_desc = description.split(";", 1)[0].strip()

    if base and base.strip().lower().endswith("by") and teacher_from_desc:
        return f"{base} {teacher_from_desc}".strip()
    return base


def _get_any_id(obj):
    for key in ("id", "_id", "slug", "subjectId", "teacherId", "topicId", "subTopicId"):
        value = _get_val(obj, key)
        if value:
            return str(value)
    return None


def _get_lecture_id(item):
    """Get lecture/chapter container ID (used as childId in API).
    This is the top-level ID of the lecture item, NOT the video content ID."""
    if not isinstance(item, dict):
        return None
    # First try to get the top-level item ID
    for key in ("_id", "id", "lectureId", "childId", "contentId"):
        value = item.get(key)
        if value:
            return str(value)
    # If no top-level ID found, try the first content item's ID
    content_list = item.get("content") or []
    for content in content_list:
        if not isinstance(content, dict):
            continue
        for key in ("_id", "id", "contentId"):
            value = content.get(key)
            if value:
                return str(value)
    return None


def _get_lecture_url(item):
    if not isinstance(item, dict):
        return None
    content_list = item.get("content") or []
    for content in content_list:
        if not isinstance(content, dict):
            continue
        video_details = content.get("videoDetails") or {}
        for key in ("videoUrl", "url", "playbackUrl", "findKey", "signedUrl", "masterUrl", "link", "src"):
            value = video_details.get(key)
            if value:
                return str(value)
        for key in ("videoUrl", "lectureUrl", "url", "video_url", "playbackUrl", "signedUrl", "downloadUrl", "streamUrl"):
            value = content.get(key)
            if value:
                return str(value)

    # Also check top-level nested videoDetails
    video_details = item.get("videoDetails") or {}
    for key in ("videoUrl", "url", "playbackUrl", "findKey", "signedUrl", "masterUrl", "link", "src"):
        value = video_details.get(key)
        if value:
            return str(value)
    for key in ("videoUrl", "lectureUrl", "url", "video_url", "playbackUrl", "signedUrl", "downloadUrl", "streamUrl"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _get_parent_id(item):
    if not isinstance(item, dict):
        return None
    for key in ("parentId", "batchId", "batch_id", "courseId", "programId"):
        value = item.get(key)
        if isinstance(value, dict):
            inner = value.get("_id") or value.get("id") or value.get("slug")
            if inner:
                return str(inner)
        if value:
            return str(value)
    content_list = item.get("content") or []
    for content in content_list:
        if not isinstance(content, dict):
            continue
        for key in ("parentId", "batchId", "batch_id", "courseId", "programId"):
            value = content.get(key)
            if isinstance(value, dict):
                inner = value.get("_id") or value.get("id") or value.get("slug")
                if inner:
                    return str(inner)
            if value:
                return str(value)
        video_details = content.get("videoDetails") or {}
        for key in ("parentId", "batchId", "batch_id", "courseId", "programId"):
            value = video_details.get(key)
            if isinstance(value, dict):
                inner = value.get("_id") or value.get("id") or value.get("slug")
                if inner:
                    return str(inner)
            if value:
                return str(value)
    return None


def _get_lecture_name(item):
    for key in ("name", "title", "lectureName", "topic"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _get_thumbnail_url(item):
    if not isinstance(item, dict):
        return None
    content_list = item.get("content") or []
    for content in content_list:
        if not isinstance(content, dict):
            continue
        video_details = content.get("videoDetails") or {}
        for key in ("image", "thumbnail", "thumbnailUrl", "thumb", "poster", "posterUrl", "imageUrl"):
            value = video_details.get(key)
            if value:
                return str(value)
        for key in ("image", "thumbnail", "thumbnailUrl", "thumb", "poster", "posterUrl", "imageUrl"):
            value = content.get(key)
            if value:
                return str(value)
    return None


def _download_thumbnail_bytes(thumb_url):
    if not thumb_url:
        return None, None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        resp = requests.get(thumb_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return None, None
        content = resp.content
        if not content:
            return None, None
        mime = resp.headers.get("content-type")
        return content, mime
    except Exception:
        return None, None


def _safe_filename(name, default="item"):
    if not name:
        return default
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name))
    safe = safe.strip("._-")
    return safe or default


def _get_video_id(item):
    """Extract actual video content ID from videoDetails (used as videoId parameter in API).
    This is the video content ID within the lecture, not the lecture container ID."""
    if not isinstance(item, dict):
        return None
    content_list = item.get("content") or []
    # First try to get videoId from content[0].videoDetails
    for content in content_list:
        if not isinstance(content, dict):
            continue
        video_details = content.get("videoDetails") or {}
        # Prioritize videoId field
        for key in ("videoId", "id", "_id"):
            value = video_details.get(key)
            if value:
                return str(value)
    # Fallback: try to find videoId from content items directly
    for content in content_list:
        if not isinstance(content, dict):
            continue
        for key in ("videoId",):
            value = content.get(key)
            if value:
                return str(value)
    # Last resort: try top-level videoId
    video_id = item.get("videoId")
    if video_id:
        return str(video_id)
    return None


def _normalize_upload_platform(raw_value):
    raw_text = str(raw_value or "").strip().lower().replace(" ", "_").replace("-", "_")
    if raw_text in ("ia", "internetarchive", "internet_archive", "archive"):
        return "internet_archive"
    if raw_text in ("none", "off", "false", "no", "0"):
        return "none"
    if raw_text in ("tg", "telegram"):
        return "telegram"
    return raw_text or "internet_archive"


def upload_to_internet_archive(file_path, identifier_base, title=None, log_callback=None, progress_callback=None):
    try:
        from mainLogic.utils.internet_archive_uploader import upload_file, identifier_dash
    except Exception as e:
        return {"ok": False, "error": f"internet_archive importer failed: {e}"}

    prefix = os.getenv("IA_IDENTIFIER_PREFIX") or "pw-khazana"
    identifier = identifier_dash(f"{prefix}-{identifier_base}")

    try:
        result_identifier = upload_file(
            file_path=file_path,
            identifier=identifier,
            title=title,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )
        return {
            "ok": True,
            "identifier": result_identifier,
            "url": f"https://archive.org/details/{result_identifier}",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _is_lecture_type(content_type):
    if not content_type:
        return False
    kind = str(content_type).upper()
    return kind in ("VIDEO", "LECTURE", "RECORDED", "VIDEO_LECTURE", "LECTURE_VIDEO")


def _get_video_url_from_content(content):
    if not isinstance(content, dict):
        return None
    video_details = content.get("videoDetails") or {}
    for key in ("videoUrl", "url", "playbackUrl", "findKey"):
        value = video_details.get(key)
        if value:
            return str(value)
    for key in ("videoUrl", "lectureUrl", "url", "video_url", "playbackUrl"):
        value = content.get(key)
        if value:
            return str(value)
    return None


def _get_file_url_from_content(content):
    if not isinstance(content, dict):
        return None
    for key in ("fileUrl", "file_url", "pdfUrl", "notesUrl", "documentUrl", "downloadUrl", "url"):
        value = content.get(key)
        if value:
            return str(value)
    return None


def _classify_asset_kind(content_type, name, url, is_video):
    context = f"{content_type or ''} {name or ''}".lower()
    if "dpp" in context:
        return "dpp_solution_video" if is_video else "dpp_notes"
    if "note" in context or (url and str(url).lower().endswith(".pdf")):
        return "notes"
    if "solution" in context and is_video:
        return "solution_video"
    return "video_asset" if is_video else "asset"


def _extract_assets(items):
    assets = []
    if isinstance(items, dict):
        for key in ("data", "items", "contents", "results"):
            if isinstance(items.get(key), list):
                items = items.get(key)
                break
    if not isinstance(items, list):
        return assets
    for item in items:
        if not isinstance(item, dict):
            continue
        content_type = item.get("contentType") or item.get("type") or item.get("itemType")
        content_list = item.get("content") or []
        if content_list and isinstance(content_list, list):
            for content in content_list:
                if not isinstance(content, dict):
                    continue
                content_type = content.get("contentType") or content.get("type") or content_type
                video_url = _get_video_url_from_content(content)
                file_url = _get_file_url_from_content(content)
                if video_url and _is_lecture_type(content_type):
                    continue
                if not video_url and not file_url:
                    continue
                content_id = (
                    content.get("id")
                    or content.get("_id")
                    or content.get("contentId")
                    or content.get("childId")
                    or content.get("videoId")
                )
                if not content_id:
                    video_details = content.get("videoDetails") or {}
                    content_id = video_details.get("id") or video_details.get("videoId")
                if not content_id:
                    content_id = _get_any_id(item) or _get_display_name(item)
                name = content.get("name") or content.get("title") or _get_lecture_name(item) or _get_display_name(item)
                is_video = bool(video_url)
                asset_url = video_url or file_url
                kind = _classify_asset_kind(content_type, name, asset_url, is_video)
                assets.append(
                    {
                        "content_id": str(content_id),
                        "content_name": name,
                        "kind": kind,
                        "file_url": asset_url,
                        "is_video": is_video,
                    }
                )
        else:
            video_url = _get_lecture_url(item)
            file_url = _get_file_url_from_content(item)
            if video_url and _is_lecture_type(content_type):
                continue
            if not video_url and not file_url:
                continue
            content_id = _get_any_id(item) or _get_display_name(item)
            name = _get_lecture_name(item) or _get_display_name(item)
            is_video = bool(video_url)
            asset_url = video_url or file_url
            kind = _classify_asset_kind(content_type, name, asset_url, is_video)
            assets.append(
                {
                    "content_id": str(content_id),
                    "content_name": name,
                    "kind": kind,
                    "file_url": asset_url,
                    "is_video": is_video,
                }
            )
    return assets


def _iter_lecture_items(items):
    if isinstance(items, dict):
        for key in ("data", "items", "contents", "results"):
            if isinstance(items.get(key), list):
                items = items.get(key)
                break
    if not isinstance(items, list):
        return []
    lectures = []
    for item in items:
        if not isinstance(item, dict):
            continue
        kind = item.get("contentType") or item.get("type") or item.get("itemType")
        has_url = _get_lecture_url(item)
        has_id = _get_lecture_id(item)
        if kind and str(kind).upper() in ("VIDEO", "LECTURE", "RECORDED", "VIDEO_LECTURE", "LECTURE_VIDEO"):
            lectures.append(item)
        elif has_url and has_id:
            lectures.append(item)
    return lectures


def _download_lecture(
    lecture_id,
    lecture_url,
    lecture_name,
    program_name,
    topic_id,
    thumb_url,
    token,
    random_id,
    base_dir,
    db_logger,
    parent_id=None,
    upload_internet_archive=False,
    delete_after_upload=False,
    upload_retries=2,
    subject_name=None,
    teacher_name=None,
    topic_name=None,
    sub_topic_name=None,
    video_id=None,
    secondary_parent_id=None,
):
    tui, log_sink = _start_task_tui(f"Preparing lecture: {lecture_name or lecture_id}")
    if not lecture_id or not lecture_url:
        error_msg = f"Missing required: lecture_id={bool(lecture_id)}, lecture_url={bool(lecture_url)}"
        debugger.error(error_msg)
        _stop_task_tui(tui, log_sink)
        return
    server_id = socket.gethostname()
    if db_logger:
        existing = db_logger.get_khazana_lecture_status_v2(program_name, topic_id, lecture_id)
        force_mode = bool(os.getenv("KHZ_FORCE_DOWNLOAD"))
        if existing and existing.get("status") == "done" and not force_mode:
            debugger.info(f"Skipping: {lecture_name or lecture_id}")
            _stop_task_tui(tui, log_sink)
            return
        # Guard against duplicate IA uploads when an identifier already exists.
        if (
            existing
            and upload_internet_archive
            and existing.get("ia_identifier")
            and not force_mode
        ):
            debugger.info(f"Skipping upload duplicate: {lecture_name or lecture_id}")
            _stop_task_tui(tui, log_sink)
            return
        # Guard against concurrent re-runs while another process is uploading.
        if (
            existing
            and upload_internet_archive
            and existing.get("status") == "uploading"
            and not force_mode
        ):
            debugger.info(f"Skipping in-progress upload: {lecture_name or lecture_id}")
            _stop_task_tui(tui, log_sink)
            return
    thumb_blob = None
    thumb_mime = None
    if thumb_url and db_logger:
        try:
            if not db_logger.has_khazana_thumbnail_v2(program_name, topic_id, lecture_id):
                thumb_blob, thumb_mime = _download_thumbnail_bytes(thumb_url)
        except Exception:
            thumb_blob = None
            thumb_mime = None
    if db_logger:
        debugger.info(f"DB: {lecture_name or lecture_id} -> downloading")
        db_logger.upsert_khazana_lecture_v2(
            program_name=program_name,
            topic_id=topic_id,
            lecture_id=lecture_id,
            topic_name=topic_name,
            subject_name=subject_name,
            teacher_name=teacher_name,
            sub_topic_name=sub_topic_name,
            lecture_name=lecture_name,
            lecture_url=lecture_url,
            thumbnail_url=thumb_url,
            thumbnail_mime=thumb_mime,
            thumbnail_size=len(thumb_blob) if thumb_blob else None,
            thumbnail_blob=thumb_blob,
            status="downloading",
            server_id=server_id,
        )
    try:
        # Validate URL format before attempting download
        if not lecture_url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid URL format: {lecture_url[:50]}...")

        # Note: Khazana lectures URLs may be stale/expired from chapter endpoint
        # The Main class will call get_key() which calls get_video_signed_url() to get fresh signed URL from API
        # Don't pre-validate with HEAD request as it may fail but get_key() will refresh it

        # For Khazana lectures, parentId in API should be program_name, not extracted parent_id
        # The extracted parent_id from item may be incorrect/outdated
        batch_name = program_name  # Always use program_name for Khazana
        effective_secondary_parent = secondary_parent_id or topic_id
        
        if tui:
            tui.set_status("Downloading media")
        
        # Get binary paths
        import shutil
        mp4decrypt_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin", "mp4decrypt")
        if not os.path.exists(mp4decrypt_path):
            mp4decrypt_path = shutil.which("mp4decrypt") or "mp4decrypt"
        ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"
        
        downloader = Main(
            id=lecture_id,
            name=lecture_name or lecture_id,
            batch_name=batch_name,
            topic_name=effective_secondary_parent,
            lecture_url=lecture_url,
            video_id=video_id,
            directory=base_dir,
            token=token,
            random_id=random_id,
            mp4d=mp4decrypt_path,
            ffmpeg=ffmpeg_path,
            verbose=False,
            tui=True,
            tui_instance=tui,
        )
        downloader.process()
        file_path = os.path.join(base_dir, f"{lecture_name or lecture_id}.mp4")
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None
        if db_logger:
            status = "done" if not upload_internet_archive else "downloaded"
            debugger.info(f"DB: {lecture_name or lecture_id} -> {status}")
            db_logger.upsert_khazana_lecture_v2(
                program_name=program_name,
                lecture_id=lecture_id,
                topic_id=topic_id,
                topic_name=topic_name,
                subject_name=subject_name,
                teacher_name=teacher_name,
                sub_topic_name=sub_topic_name,
                lecture_name=lecture_name,
                lecture_url=lecture_url,
                thumbnail_url=thumb_url,
                thumbnail_mime=thumb_mime,
                thumbnail_size=len(thumb_blob) if thumb_blob else None,
                thumbnail_blob=thumb_blob,
                status=status,
                server_id=server_id,
                file_path=file_path if os.path.exists(file_path) else None,
                file_size=file_size,
                error=None,
            )
        debugger.success(f"Downloaded: {lecture_name or lecture_id}")
        if upload_internet_archive:
            if tui:
                tui.set_status("Uploading to IA")
                tui.setup_upload_progress()
            if db_logger:
                debugger.info(f"DB: {lecture_name or lecture_id} -> uploading")
                db_logger.upsert_khazana_lecture_v2(
                    program_name=program_name,
                    lecture_id=lecture_id,
                    topic_id=topic_id,
                    status="uploading",
                    server_id=server_id,
                    file_path=file_path if os.path.exists(file_path) else None,
                    file_size=file_size,
                )
            if not file_path or not os.path.exists(file_path):
                raise RuntimeError("Downloaded file not found for upload")
            if os.path.getsize(file_path) == 0:
                raise RuntimeError("Downloaded file is empty")
            last_error = None
            upload_result = None
            for attempt in range(1, max(1, upload_retries) + 1):
                debugger.info(f"IA: upload {attempt}/{max(1, upload_retries)}")
                upload_result = upload_to_internet_archive(
                    file_path,
                    identifier_base=f"{program_name}-{lecture_id}",
                    title=lecture_name or lecture_id,
                    log_callback=(lambda msg: tui.log(msg, "INFO")) if tui else None,
                    progress_callback=(
                        (lambda pct: tui.update_upload_progress(pct, f"IA Upload: {pct}%"))
                        if tui else None
                    ),
                )
                if upload_result.get("ok"):
                    break
                last_error = upload_result.get("error") or "upload_failed"
                debugger.error(f"IA upload failed (attempt {attempt}): {last_error}")
                if attempt < upload_retries:
                    time.sleep(2)

            if upload_result and upload_result.get("ok"):
                if tui:
                    tui.finish_upload_progress(success=True)
                identifier = upload_result.get("identifier")
                url = upload_result.get("url")
                db_ok = True
                if db_logger:
                    debugger.info(f"DB: {lecture_name or lecture_id} -> done (IA)")
                    try:
                        db_logger.upsert_khazana_lecture_v2(
                            program_name=program_name,
                            lecture_id=lecture_id,
                            topic_id=topic_id,
                            status="done",
                            server_id=server_id,
                            file_path=file_path if os.path.exists(file_path) else None,
                            file_size=file_size,
                            ia_identifier=identifier,
                            ia_url=url,
                        )
                    except Exception as e:
                        db_ok = False
                        debugger.error(f"Failed to update DB after IA upload: {e}")
                if file_path and os.path.exists(file_path):
                    if not db_ok:
                        debugger.warning("Skipping local file deletion because DB update failed.")
                    else:
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            debugger.error(f"Failed to delete file after upload: {e}")
                        else:
                            if db_logger:
                                try:
                                    db_logger.upsert_khazana_lecture_v2(
                                        program_name=program_name,
                                        lecture_id=lecture_id,
                                        topic_id=topic_id,
                                        status="done",
                                        file_path=None,
                                        file_size=None,
                                    )
                                except Exception as e:
                                    debugger.error(f"Failed to update DB after deleting local file: {e}")
                debugger.info(f"Uploaded to Internet Archive: {lecture_name or lecture_id}")
            else:
                if tui:
                    tui.finish_upload_progress(success=False)
                error_text = last_error or "upload_failed"
                if db_logger:
                    db_logger.upsert_khazana_lecture_v2(
                        program_name=program_name,
                        lecture_id=lecture_id,
                        topic_id=topic_id,
                        status="failed",
                        server_id=server_id,
                        file_path=file_path if os.path.exists(file_path) else None,
                        file_size=file_size,
                        error=error_text,
                    )
                debugger.error(f"Internet Archive upload failed: {error_text}")
                if delete_after_upload and file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        debugger.error(f"Failed to delete file after failed upload: {e}")
                    else:
                        if db_logger:
                            try:
                                db_logger.upsert_khazana_lecture_v2(
                                    program_name=program_name,
                                    lecture_id=lecture_id,
                                    topic_id=topic_id,
                                    status="failed",
                                    file_path=None,
                                    file_size=None,
                                )
                            except Exception as e:
                                debugger.error(f"Failed to clear file_path in DB after deletion: {e}")
    except Exception as e:
        if db_logger:
            db_logger.upsert_khazana_lecture_v2(
                program_name=program_name,
                lecture_id=lecture_id,
                topic_id=topic_id,
                topic_name=topic_name,
                subject_name=subject_name,
                teacher_name=teacher_name,
                sub_topic_name=sub_topic_name,
                lecture_name=lecture_name,
                lecture_url=lecture_url,
                status="failed",
                server_id=server_id,
                error=str(e),
            )
        debugger.error(f"Download failed for {lecture_name or lecture_id}: {e}")
    finally:
        _stop_task_tui(tui, log_sink)


def _download_asset(
    content_id,
    content_name,
    kind,
    file_url,
    is_video,
    program_name,
    topic_id,
    base_dir,
    token,
    random_id,
    db_logger,
    upload_internet_archive=False,
    delete_after_upload=False,
    upload_retries=2,
    subject_name=None,
    teacher_name=None,
    topic_name=None,
    sub_topic_name=None,
):
    tui, log_sink = _start_task_tui(f"Preparing asset: {content_name or content_id}")
    if not content_id or not file_url:
        _stop_task_tui(tui, log_sink)
        return
    server_id = socket.gethostname()
    if db_logger:
        existing = db_logger.get_khazana_asset_status(program_name, content_id, kind)
        force_mode = bool(os.getenv("KHZ_FORCE_DOWNLOAD"))
        if existing and existing.get("status") == "done" and not force_mode:
            debugger.info(f"Skipping asset: {content_name or content_id}")
            _stop_task_tui(tui, log_sink)
            return
        if (
            existing
            and upload_internet_archive
            and existing.get("ia_identifier")
            and not force_mode
        ):
            debugger.info(f"Skipping asset upload duplicate: {content_name or content_id}")
            _stop_task_tui(tui, log_sink)
            return
        if (
            existing
            and upload_internet_archive
            and existing.get("status") == "uploading"
            and not force_mode
        ):
            debugger.info(f"Skipping asset in-progress upload: {content_name or content_id}")
            _stop_task_tui(tui, log_sink)
            return
        debugger.info(f"DB: asset downloading ({content_name or content_id})")
        db_logger.upsert_khazana_asset(
            program_name=program_name,
            content_id=content_id,
            kind=kind,
            content_name=content_name,
            file_url=file_url,
            status="downloading",
            server_id=server_id,
            subject_name=subject_name,
            teacher_name=teacher_name,
            topic_name=topic_name,
            sub_topic_name=sub_topic_name,
        )

    asset_dir = os.path.join(base_dir, "khazana_assets")
    os.makedirs(asset_dir, exist_ok=True)
    safe_name = _safe_filename(content_name or content_id)
    if is_video:
        file_path = os.path.join(asset_dir, f"{safe_name}.mp4")
    else:
        ext = os.path.splitext(urlparse(file_url).path)[1]
        if not ext:
            ext = ".pdf"
        file_path = os.path.join(asset_dir, f"{safe_name}{ext}")

    try:
        if is_video:
            if tui:
                tui.set_status("Starting asset video download")
            
            # Get binary paths
            import shutil
            mp4decrypt_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin", "mp4decrypt")
            if not os.path.exists(mp4decrypt_path):
                mp4decrypt_path = shutil.which("mp4decrypt") or "mp4decrypt"
            ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"
            
            downloader = Main(
                id=content_id,
                name=safe_name,
                batch_name=program_name,
                topic_name=topic_name,
                lecture_url=file_url,
                directory=asset_dir,
                token=token,
                random_id=random_id,
                mp4d=mp4decrypt_path,
                ffmpeg=ffmpeg_path,
                verbose=False,
                tui=True,
                tui_instance=tui,
            )
            downloader.process()
        else:
            if tui:
                tui.set_status("Downloading file asset")
            resp = requests.get(file_url, stream=True, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(f"Asset download failed with status {resp.status_code}")
            with open(file_path, "wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None
        if db_logger:
            status = "done" if not upload_internet_archive else "downloaded"
            debugger.info(f"DB: asset status -> {status} ({content_name or content_id})")
            db_logger.upsert_khazana_asset(
                program_name=program_name,
                content_id=content_id,
                kind=kind,
                content_name=content_name,
                file_url=file_url,
                file_path=file_path if os.path.exists(file_path) else None,
                file_size=file_size,
                status=status,
                server_id=server_id,
                subject_name=subject_name,
                teacher_name=teacher_name,
                topic_name=topic_name,
                sub_topic_name=sub_topic_name,
            )
        debugger.success(f"Downloaded asset: {content_name or content_id}")

        if upload_internet_archive:
            if tui:
                tui.set_status("Uploading asset to IA")
                tui.setup_upload_progress()
            if db_logger:
                debugger.info(f"DB: asset status -> uploading ({content_name or content_id})")
                db_logger.upsert_khazana_asset(
                    program_name=program_name,
                    content_id=content_id,
                    kind=kind,
                    status="uploading",
                    server_id=server_id,
                    file_path=file_path if os.path.exists(file_path) else None,
                    file_size=file_size,
                )
            if not file_path or not os.path.exists(file_path):
                raise RuntimeError("Downloaded asset file not found for upload")
            if os.path.getsize(file_path) == 0:
                raise RuntimeError("Downloaded asset file is empty")
            last_error = None
            upload_result = None
            for attempt in range(1, max(1, upload_retries) + 1):
                debugger.info(f"IA: upload attempt {attempt}/{max(1, upload_retries)} (asset {content_name or content_id})")
                upload_result = upload_to_internet_archive(
                    file_path,
                    identifier_base=f"{program_name}-{content_id}-{kind}",
                    title=content_name or content_id,
                    log_callback=(lambda msg: tui.log(msg, "INFO")) if tui else None,
                    progress_callback=(
                        (lambda pct: tui.update_upload_progress(pct, f"IA Upload: {pct}%"))
                        if tui else None
                    ),
                )
                if upload_result.get("ok"):
                    break
                last_error = upload_result.get("error") or "upload_failed"
                debugger.error(f"IA upload failed (attempt {attempt}): {last_error}")
                if attempt < upload_retries:
                    time.sleep(2)

            if upload_result and upload_result.get("ok"):
                if tui:
                    tui.finish_upload_progress(success=True)
                identifier = upload_result.get("identifier")
                url = upload_result.get("url")
                db_ok = True
                if db_logger:
                    try:
                        db_logger.upsert_khazana_asset(
                            program_name=program_name,
                            content_id=content_id,
                            kind=kind,
                            status="done",
                            server_id=server_id,
                            file_path=file_path if os.path.exists(file_path) else None,
                            file_size=file_size,
                            ia_identifier=identifier,
                            ia_url=url,
                        )
                    except Exception as e:
                        db_ok = False
                        debugger.error(f"Failed to update DB after IA upload: {e}")
                if file_path and os.path.exists(file_path):
                    if not db_ok:
                        debugger.warning("Skipping local file deletion because DB update failed.")
                    else:
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            debugger.error(f"Failed to delete asset after upload: {e}")
                        else:
                            if db_logger:
                                try:
                                    db_logger.upsert_khazana_asset(
                                        program_name=program_name,
                                        content_id=content_id,
                                        kind=kind,
                                        status="done",
                                        file_path=None,
                                        file_size=None,
                                    )
                                except Exception as e:
                                    debugger.error(f"Failed to update DB after deleting asset: {e}")
                debugger.info(f"Uploaded asset to Internet Archive: {content_name or content_id}")
            else:
                if tui:
                    tui.finish_upload_progress(success=False)
                error_text = last_error or "upload_failed"
                if db_logger:
                    db_logger.upsert_khazana_asset(
                        program_name=program_name,
                        content_id=content_id,
                        kind=kind,
                        status="failed",
                        server_id=server_id,
                        file_path=file_path if os.path.exists(file_path) else None,
                        file_size=file_size,
                        error=error_text,
                    )
                debugger.error(f"Internet Archive upload failed: {error_text}")
                if delete_after_upload and file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        debugger.error(f"Failed to delete asset after failed upload: {e}")
                    else:
                        if db_logger:
                            try:
                                db_logger.upsert_khazana_asset(
                                    program_name=program_name,
                                    content_id=content_id,
                                    kind=kind,
                                    status="failed",
                                    file_path=None,
                                    file_size=None,
                                )
                            except Exception as e:
                                debugger.error(f"Failed to clear asset file_path in DB after deletion: {e}")
    except Exception as e:
        if db_logger:
            db_logger.upsert_khazana_asset(
                program_name=program_name,
                content_id=content_id,
                kind=kind,
                content_name=content_name,
                file_url=file_url,
                status="failed",
                server_id=server_id,
                file_path=file_path if os.path.exists(file_path) else None,
                error=str(e),
            )
        debugger.error(f"Asset download failed for {content_name or content_id}: {e}")
    finally:
        _stop_task_tui(tui, log_sink)


def main():
    batch_api, random_id = _select_user_and_init_api(prefs)
    program_name = None
    selected_batch_name = None
    packs = _list_khazana_packs(batch_api)
    if packs:
        print("Available batches (Khazana-enabled):")
        for idx, pack in enumerate(packs, start=1):
            print(f"  {idx}. {pack.get('name')} (slug={pack.get('slug')})")
        print("  m. Manual program id input")
        choice = input("Select batch number [1]: ").strip()
        if not choice:
            choice = "1"
        if choice.lower() != "m":
            try:
                i = int(choice) - 1
                if 0 <= i < len(packs):
                    selected = packs[i]
                    selected_batch_name = selected.get("name")
                    program_name = selected.get("program_id")
                else:
                    debugger.warning("Invalid batch selection.")
            except ValueError:
                debugger.warning("Invalid input for batch selection.")

    if not program_name:
        batch_hint = input("Enter batch slug/id to auto-detect Khazana program (or press Enter to input program id): ").strip()
        if batch_hint:
            program_name = _resolve_khazana_program_id(batch_api, batch_hint)
        if not program_name:
            program_name = input("Enter Khazana program id (khazanaProgramId): ").strip()
        if not program_name:
            debugger.error("Khazana program id is required.")
            return
    else:
        debugger.info(f"Selected batch: {selected_batch_name}")

    token = getattr(batch_api, "token", None) or os.getenv("PWDL_TOKEN")
    if not token:
        debugger.error("Token not found. Please select a profile with a valid token.")
        return

    base_dir = prefs.get("extension_download_dir") or "./"
    os.makedirs(base_dir, exist_ok=True)

    db_logger = None
    if os.getenv("PWDL_DB_URL"):
        try:
            from mainLogic.utils import mysql_logger as db_logger
            db_logger.init(None)
            db_logger.ensure_schema()
            debugger.info("DB logger enabled.")
        except Exception as e:
            debugger.error(f"Failed to init DB logger: {e}")
            db_logger = None

    upload_platform_raw = (
        prefs.get("upload_platform")
        or os.getenv("KHZ_UPLOAD_PLATFORM")
        or os.getenv("UPLOAD_PLATFORM")
        or "internet_archive"
    )
    upload_platform = _normalize_upload_platform(upload_platform_raw)
    if upload_platform == "telegram":
        debugger.warning("Telegram upload is not supported in khazana_dl; disabling uploads.")
        upload_platform = "none"
    upload_internet_archive = upload_platform == "internet_archive"
    delete_after_upload = bool(prefs.get("delete_after_upload", True))
    try:
        upload_retries = int(os.getenv("KHZ_UPLOAD_RETRIES", "2"))
    except Exception:
        upload_retries = 2
    if upload_internet_archive and (not os.getenv("IA_ACCESS_KEY") or not os.getenv("IA_SECRET_KEY")):
        debugger.warning("Internet Archive upload enabled; IA_ACCESS_KEY/IA_SECRET_KEY not set (will rely on ia.ini if present).")

    try:
        subjects = batch_api.process("details", khazana=True, program_name=program_name)
    except Exception as e:
        debugger.error(f"Failed to load Khazana subjects: {e}")
        return

    subjects = subjects or []
    subjects_sel = _pick_from_list(subjects, "Available subjects:", _get_display_name)
    if not subjects_sel:
        debugger.error("No subjects selected.")
        return

    for subject in subjects_sel:
        subject_id = _get_any_id(subject) or _get_display_name(subject)
        subject_label = _get_display_name(subject)
        try:
            teachers = batch_api.process(
                "subject",
                khazana=True,
                program_name=program_name,
                subject_name=subject_id,
            )
        except Exception as e:
            debugger.error(f"Failed to load teachers for subject {subject_label}: {e}")
            continue

        teachers = teachers or []
        teachers_sel = _pick_from_list(teachers, f"Teachers for {subject_label}:", _get_teacher_label)
        if not teachers_sel:
            continue

        for teacher in teachers_sel:
            teacher_id = _get_any_id(teacher) or _get_display_name(teacher)
            teacher_label = _get_teacher_label(teacher)
            try:
                topics = batch_api.process(
                    "topics",
                    khazana=True,
                    program_name=program_name,
                    subject_name=subject_id,
                    teacher_name=teacher_id,
                )
            except Exception as e:
                debugger.error(f"Failed to load topics for {teacher_label}: {e}")
                continue

            topics = topics or []
            topics_sel = _pick_from_list(topics, f"Topics for {teacher_label}:", _get_display_name)
            if not topics_sel:
                continue

            for topic in topics_sel:
                topic_id = _get_any_id(topic) or _get_display_name(topic)
                topic_label = _get_display_name(topic)
                try:
                    sub_topics = batch_api.process(
                        "sub_topic",
                        khazana=True,
                        program_name=program_name,
                        subject_name=subject_id,
                        teacher_name=teacher_id,
                        topic_name=topic_id,
                    )
                except Exception as e:
                    debugger.error(f"Failed to load sub-topics for {topic_label}: {e}")
                    continue

                sub_topics = sub_topics or []
                sub_topics_sel = _pick_from_list(sub_topics, f"Sub-topics for {topic_label}:", _get_display_name)
                if not sub_topics_sel:
                    continue

                for sub_topic in sub_topics_sel:
                    sub_topic_id = _get_any_id(sub_topic) or _get_display_name(sub_topic)
                    sub_topic_label = _get_display_name(sub_topic)
                    try:
                        chapter_contents = batch_api.process(
                            "chapter",
                            khazana=True,
                            program_name=program_name,
                            subject_name=subject_id,
                            teacher_name=teacher_id,
                            topic_name=topic_id,
                            sub_topic_name=sub_topic_id,
                        )
                    except Exception as e:
                        debugger.error(f"Failed to load lectures for {sub_topic_label}: {e}")
                        continue

                    lecture_items = _iter_lecture_items(chapter_contents)
                    assets = _extract_assets(chapter_contents)
                    if not lecture_items and not assets:
                        debugger.warning(f"No lectures or assets found in {sub_topic_label}.")
                        continue

                    if lecture_items:
                        print(f"Lectures in {sub_topic_label}:")
                        for idx, item in enumerate(lecture_items, start=1):
                            name = _get_lecture_name(item) or _get_lecture_id(item) or f"lecture-{idx}"
                            print(f"  {idx}. {name}")
                        choice = input("Download ALL lectures here? (y/N): ").strip().lower()
                        if choice == "y":
                            selected_lectures = lecture_items
                        else:
                            nums = input("Enter lecture number(s) comma-separated: ").strip()
                            selected_lectures = []
                            if nums:
                                for part in nums.split(","):
                                    try:
                                        i = int(part.strip()) - 1
                                        if 0 <= i < len(lecture_items):
                                            selected_lectures.append(lecture_items[i])
                                    except ValueError:
                                        continue

                        for item in selected_lectures:
                            lecture_id = _get_lecture_id(item)
                            lecture_url = _get_lecture_url(item)
                            lecture_name = _get_lecture_name(item)
                            thumb_url = _get_thumbnail_url(item)
                            parent_id = _get_parent_id(item)
                            video_id = _get_video_id(item)
                            _download_lecture(
                                lecture_id=lecture_id,
                                lecture_url=lecture_url,
                                lecture_name=lecture_name,
                                program_name=program_name,
                                topic_id=topic_id,
                                thumb_url=thumb_url,
                                token=token,
                                random_id=random_id,
                                base_dir=base_dir,
                                db_logger=db_logger,
                                parent_id=parent_id,
                                upload_internet_archive=upload_internet_archive,
                                delete_after_upload=delete_after_upload,
                                upload_retries=upload_retries,
                                subject_name=subject_label,
                                teacher_name=teacher_label,
                                topic_name=topic_label,
                                sub_topic_name=sub_topic_label,
                                video_id=video_id,
                                secondary_parent_id=subject_id,
                            )

                    if assets:
                        choice = input("Download notes/DPP assets in this sub-topic? (y/N): ").strip().lower()
                        if choice == "y":
                            for asset in assets:
                                _download_asset(
                                    content_id=asset.get("content_id"),
                                    content_name=asset.get("content_name"),
                                    kind=asset.get("kind"),
                                    file_url=asset.get("file_url"),
                                    is_video=asset.get("is_video"),
                                    program_name=program_name,
                                    topic_id=topic_id,
                                    base_dir=base_dir,
                                    token=token,
                                    random_id=random_id,
                                    db_logger=db_logger,
                                    upload_internet_archive=upload_internet_archive,
                                    delete_after_upload=delete_after_upload,
                                    upload_retries=upload_retries,
                                    subject_name=subject_label,
                                    teacher_name=teacher_label,
                                    topic_name=topic_label,
                                    sub_topic_name=sub_topic_label,
                                )


if __name__ == "__main__":
    main()
