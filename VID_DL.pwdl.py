import argparse
import json
import os
import glob
import re
import shutil
import socket
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from operator import attrgetter # For easier sorting
import asyncio

import pytz # Make sure to pip install pytz if you haven't already
import requests
from beta.batch_scraper_2.module import ScraperModule
from beta.batch_scraper_2.Endpoints import Endpoints
from mainLogic.utils.glv_var import PREFS_FILE

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
        TransferSpeedColumn,
    )
except Exception:
    Console = None
    Progress = None

# Assuming mainLogic.downloader.py contains a function named 'main'
from mainLogic.downloader import main as downloader # Renamed to avoid confusion with internal 'main'
from mainLogic.utils.gen_utils import generate_safe_folder_name

# --- 1. Set up argparse (minimal; keep interactive flow only) ---
parser = argparse.ArgumentParser(description="Scrape and upload lectures with interactive selection.")
# CLI options: support login, provide phone, select user non-interactively, and list batches for all users
parser.add_argument('--login', action='store_true', help='Run login flow and add profile to preferences')
parser.add_argument('--phone', type=str, help='Phone number to pass to login (optional)')
parser.add_argument('--user', type=str, help='Select user profile by index (1-based), id, or name (non-interactive)')
parser.add_argument('--all-batches', action='store_true', help='List purchased batches for all stored user profiles and exit')
args = parser.parse_args()

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

prefs = ScraperModule.prefs # Still keep prefs for other potential settings
debugger = ScraperModule.debugger

# --- Multi-user preference support ---
# Expect prefs to have a 'users' key with a list of {name, token or access_token, random_id?}
def _select_user_and_init_api(prefs):
    users = prefs.get('users', []) if isinstance(prefs, dict) else []
    if not users:
        # fallback to existing module-level batch_api
        return ScraperModule.batch_api

    # Non-interactive user selection via CLI arg
    if getattr(args, 'user', None):
        key = str(args.user).strip()
        # numeric index
        try:
            idx = int(key) - 1
            if 0 <= idx < len(users):
                chosen = users[idx]
            else:
                debugger.error("--user index out of range, defaulting to first user")
                chosen = users[0]
        except Exception:
            # match by id or name
            found = None
            for u in users:
                if str(u.get('id')) == key or str(u.get('name')) == key or str(u.get('username')) == key:
                    found = u
                    break
            chosen = found or users[0]
        # proceed to init below

    print("Multiple user profiles found. Select user to use for API requests:")
    for idx, u in enumerate(users, start=1):
        uname = u.get('name') or u.get('username') or f"user-{idx}"
        token_preview = (u.get('access_token') or u.get('token') or '')[:8]
        print(f"  {idx}. {uname} (token startswith: {token_preview}...)")
    print("  a. Add new user")
    print("  q. Quit")
    sel = input("Choose user (number) or action [1]: ").strip()
    if not sel:
        sel = '1'
    if sel.lower() == 'q':
        print("Exiting.")
        exit()
    if sel.lower() == 'a':
        name = input("Enter profile name: ").strip() or f"user-{len(users)+1}"
        token = input("Enter access token (Bearer token string): ").strip()
        random_id = input("Enter random_id (optional): ").strip() or None
        new_user = {'name': name, 'access_token': token}
        if random_id:
            new_user['random_id'] = random_id
        users.append(new_user)
        # persist back to prefs file
        try:
            with open(PREFS_FILE, 'r', encoding='utf-8') as f:
                pf = json.load(f)
        except Exception:
            pf = prefs if isinstance(prefs, dict) else {}
        pf['users'] = users
        try:
            with open(PREFS_FILE, 'w', encoding='utf-8') as f:
                json.dump(pf, f, indent=2)
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

    token = chosen.get('access_token') or chosen.get('token') or chosen.get('token_config', {}).get('access_token')
    random_id = chosen.get('random_id') or chosen.get('randomId') or chosen.get('token_config', {}).get('random_id')
    if not token:
        debugger.error("Selected profile does not have a token. Please update preferences.")
        return ScraperModule.batch_api

    try:
        if random_id:
            return Endpoints(verbose=False).set_token(token, random_id=random_id)
        else:
            return Endpoints(verbose=False).set_token(token)
    except Exception as e:
        debugger.error(f"Failed to initialize API with selected profile: {e}")
        return ScraperModule.batch_api


# Initialize batch_api using selected user if available
# If login requested via CLI, run login flow and reload prefs
if getattr(args, 'login', False):
    try:
        from mainLogic.startup.Login.call_login import LoginInterface
        LoginInterface.cli(getattr(args, 'phone', None))
        # reload prefs from file
        try:
            with open(PREFS_FILE, 'r', encoding='utf-8') as f:
                prefs = json.load(f)
        except Exception:
            prefs = ScraperModule.prefs
    except Exception as e:
        debugger.error(f"Login failed: {e}")

# If --all-batches requested, list batches for every stored profile and exit
if getattr(args, 'all_batches', False):
    users = prefs.get('users', []) if isinstance(prefs, dict) else []
    if not users:
        debugger.error("No user profiles found in preferences.")
        exit()
    for u in users:
        name = u.get('name') or u.get('username') or str(u.get('id'))
        token = u.get('access_token') or u.get('token') or (u.get('token', {}) if isinstance(u.get('token'), dict) else None)
        print(f"\nBatches for profile: {name}")
        try:
            if isinstance(token, dict):
                access = token.get('access_token')
            else:
                access = u.get('access_token') or u.get('token')
            if not access:
                print("  No access token for this profile, skipping.")
                continue
            api = Endpoints(verbose=False).set_token(access)
            batches = api.get_purchased_batches(all_pages=True)
            for b in batches:
                print(f"  - {b.get('name')} (slug={b.get('slug')}, id={b.get('_id')})")
        except Exception as e:
            print(f"  Failed to fetch batches: {e}")
    exit()

batch_api = _select_user_and_init_api(prefs)

# --- Interactive defaults (no CLI flags) ---
batch_input = None
subjects_of_interest = set()

download_enabled = True
upload_telegram = True
upload_as_video = bool(prefs.get("upload_as_video", False))
base_download_directory = prefs.get("extension_download_dir") or "./"
server_id = socket.gethostname()
min_free_gb = float(prefs.get("min_free_gb", 2.0))
lock_ttl_min = int(prefs.get("lock_ttl_min", 120))
delete_after_upload = bool(prefs.get("delete_after_upload", True))
force_reupload = bool(prefs.get("force_reupload", False))

specific_date_filter_utc = None


# Ensure batch_api is initialized before use
if batch_api is None:
    debugger.error("ScraperModule.batch_api is not initialized. Please check token setup.")
    exit()

batch_slug = batch_input
batch_id = None
batch_display_name = None

try:
    purchased_batches = batch_api.get_purchased_batches(all_pages=True)
except Exception as e:
    purchased_batches = []
    debugger.error(f"Failed to load purchased batches: {e}")

# Interactive batch selection (always on)
if not purchased_batches:
    debugger.error("No purchased batches available for selection.")
    exit()
print("Purchased batches:")
for idx, b in enumerate(purchased_batches, start=1):
    print(f"{idx}. {b.get('name','')}  (slug={b.get('slug')}, id={b.get('_id')})")
choice = input("Enter batch number: ").strip()
if not choice:
    debugger.error("No batch selected.")
    exit()
try:
    i = int(choice) - 1
    if 0 <= i < len(purchased_batches):
        sel = purchased_batches[i]
        batch_input = sel.get('slug') or sel.get('_id')
    else:
        debugger.error("Invalid batch number selected.")
        exit()
except ValueError:
    debugger.error("Invalid input. Enter a batch number.")
    exit()

# DB logging (auto-enable when DB URL is present)
db_logger = None
if os.getenv("PWDL_DB_URL"):
    try:
        from mainLogic.utils import mysql_logger as db_logger
        debugger.info("DB logger enabled.")
    except Exception as e:
        debugger.error(f"Failed to import DB logger: {e}")
        db_logger = None

def ensure_free_space(path, min_gb):
    try:
        usage = shutil.disk_usage(path)
        free_gb = usage.free / (1024 ** 3)
        if free_gb < min_gb:
            debugger.error(f"Low disk space: {free_gb:.2f} GB free (< {min_gb} GB required).")
            return False
        return True
    except Exception as e:
        debugger.error(f"Disk space check failed: {e}")
        return False

if db_logger:
    try:
        db_logger.init(None)
        db_logger.ensure_schema()
    except Exception as e:
        debugger.error(f"DB init failed: {e}")
        db_logger = None

_local_upload_cache = None
_local_upload_cache_path = None
_local_upload_cache_dirty = False

def _get_upload_cache_path():
    cache_dir = base_download_directory or "."
    return os.path.join(cache_dir, ".pwl_upload_cache.json")

def _load_upload_cache():
    global _local_upload_cache, _local_upload_cache_path
    if _local_upload_cache is not None:
        return _local_upload_cache
    _local_upload_cache_path = _get_upload_cache_path()
    try:
        with open(_local_upload_cache_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            _local_upload_cache = data if isinstance(data, dict) else {}
    except FileNotFoundError:
        _local_upload_cache = {}
    except Exception as e:
        debugger.warning(f"Failed to load local upload cache: {e}")
        _local_upload_cache = {}
    return _local_upload_cache

def _save_upload_cache():
    global _local_upload_cache_dirty
    if not _local_upload_cache_dirty or _local_upload_cache is None:
        return
    path = _local_upload_cache_path or _get_upload_cache_path()
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(_local_upload_cache, handle, indent=2, sort_keys=True)
        _local_upload_cache_dirty = False
    except Exception as e:
        debugger.warning(f"Failed to save local upload cache: {e}")

def _local_is_upload_done(batch_id, lecture_id):
    if not (batch_id and lecture_id):
        return False
    data = _load_upload_cache()
    return str(lecture_id) in (data.get(str(batch_id)) or {})

def _local_mark_upload_done(batch_id, lecture_id, lecture_name=None, chapter_name=None):
    global _local_upload_cache_dirty
    if not (batch_id and lecture_id):
        return
    data = _load_upload_cache()
    batch_key = str(batch_id)
    lecture_key = str(lecture_id)
    batch = data.setdefault(batch_key, {})
    if lecture_key in batch:
        return
    batch[lecture_key] = {
        "lecture_name": lecture_name,
        "chapter_name": chapter_name,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    _local_upload_cache_dirty = True
    _save_upload_cache()

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

def _fetch_chapter_lectures(batch_api, batch_id, batch_slug, subject_obj, subject_slug, chapter_obj):
    subject_api_id = _get_subject_api_id(subject_obj)
    tag_ids = []
    primary_tag = _get_chapter_tag_id(chapter_obj)
    if primary_tag:
        tag_ids.append(primary_tag)
    chapter_id = getattr(chapter_obj, "id", None) or getattr(chapter_obj, "_id", None)
    if chapter_id and chapter_id not in tag_ids:
        tag_ids.append(chapter_id)

    if subject_api_id and tag_ids and hasattr(batch_api, "get_batch_chapter_lectures_v3"):
        for tag_id in tag_ids:
            v3_lectures = batch_api.get_batch_chapter_lectures_v3(
                batch_id=batch_id,
                subject_id=subject_api_id,
                tag_id=tag_id,
            )
            if v3_lectures:
                return v3_lectures
        debugger.warning("v3 tagId returned 0 lectures; falling back to v2 tag-name filter.")
    if not subject_api_id or not tag_ids:
        debugger.warning("Missing subjectId/tagId; falling back to v2 tag-name filter.")

    v2_lectures = batch_api.get_batch_chapters(
        batch_name=batch_slug,
        subject_name=subject_slug,
        chapter_name=getattr(chapter_obj, "name", None) or str(chapter_obj),
    )
    chapter_name = getattr(chapter_obj, "name", None)
    if chapter_name:
        filtered = _filter_lectures_by_tag_name(v2_lectures, chapter_name)
        if filtered:
            return filtered
        if v2_lectures:
            debugger.warning("v2 tag-name filter returned 0 matches; returning empty list to avoid wrong lectures.")
        return []
    return v2_lectures

def _get_env_text(var_name):
    value = os.getenv(var_name)
    return value.strip() if isinstance(value, str) else value

def _get_env_bool(var_name, default=False):
    value = _get_env_text(var_name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

if upload_telegram:
    if not _get_env_text("TELEGRAM_BOT_TOKEN") or not _get_env_text("TELEGRAM_CHAT_ID"):
        debugger.error("Telegram upload enabled but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID is missing.")
        exit()

def build_caption(batch_slug, subject_slug, subject_name, chapter_name, lecture_name, lecture_id, start_time, teacher_names=None, course_name=None):
    def _normalize_teacher_names(tn):
        if not tn:
            return None
        # bytes -> str
        if isinstance(tn, (bytes, bytearray)):
            try:
                return tn.decode('utf-8')
            except Exception:
                return str(tn)
        # list/tuple -> join
        if isinstance(tn, (list, tuple)):
            parts = []
            for item in tn:
                if not item:
                    continue
                if isinstance(item, (bytes, bytearray)):
                    try:
                        parts.append(item.decode('utf-8'))
                    except Exception:
                        parts.append(str(item))
                elif isinstance(item, dict):
                    parts.append(item.get('name') or item.get('fullName') or str(item))
                else:
                    parts.append(str(item))
            return ", ".join(parts) if parts else None
        # dict-like with name
        if isinstance(tn, dict):
            return tn.get('name') or tn.get('fullName') or str(tn)
        # fallback
        return str(tn)
    def clean_display_name(value, fallback=None):
        if not value:
            value = fallback
        if not value:
            return ""
        text = str(value).strip()
        # Remove control/non-printable characters
        text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text)
        # Remove common error prefixes that sometimes leak from APIs
        text = re.sub(r"^(ERROR[:\-\s]+|ERR[:\-\s]+|Error[:\-\s]+|Failed[:\-\s]+|Exception[:\-\s]+)", "", text, flags=re.IGNORECASE)
        # Remove phrases that look like HTTP/API error messages
        text = re.sub(r"\b(could not fetch|couldn't fetch|fetch failed|request failed|not found|404|error)\b", "", text, flags=re.IGNORECASE)
        # If slug-like with trailing numeric id, drop the id suffix.
        text = re.sub(r"-[0-9]{4,}$", "", text)
        # Trim stray punctuation/colons/hyphens at ends
        text = re.sub(r"^[\-:\s]+|[\-:\s]+$", "", text)
        # Replace underscores with spaces and collapse again
        text = text.replace("_", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def clean_subject_name(name, slug):
        name_text = clean_display_name(name, slug)
        if name_text == slug:
            return clean_display_name(slug, slug)
        return name_text

    def esc(text):
        return (str(text)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;"))

    bot_name = _get_env_text("TELEGRAM_BOT_NAME") or "YourBotName"
    if bot_name.startswith("@"):  # Normalize handle for display
        bot_name = bot_name[1:]
    course_text = clean_display_name(course_name or batch_slug)
    teacher_text = _normalize_teacher_names(teacher_names) or None
    if teacher_text:
        # Minimal cleanup only: remove control chars, normalize whitespace and underscores.
        teacher_text = re.sub(r"[\x00-\x1f\x7f]+", " ", str(teacher_text))
        teacher_text = re.sub(r"\s+", " ", teacher_text).replace("_", " ").strip()
        if not teacher_text:
            teacher_text = "TBA"
    else:
        teacher_text = "TBA"
    subject_text = clean_subject_name(subject_name, subject_slug)
    chapter_text = clean_display_name(chapter_name)
    lecture_text = clean_display_name(lecture_name)

    start_text = None
    if start_time:
        if hasattr(start_time, "strftime"):
            start_text = start_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            start_text = str(start_time)

    quality_text = _get_env_text("TELEGRAM_QUALITY") or "1080p"
    lines = [
        "╭❖━━━━━━━━━━━━━━━━━━━━❖╮",
        "      🎯 LECTURE UPDATE",
        "╰❖━━━━━━━━━━━━━━━━━━━━❖╯",
        "",
        f"📘 Course  : {esc(course_text)}",
        f"📚 Subject : {esc(subject_text)}",
        f"📌 Chapter : {esc(chapter_text)}",
        f"🎯 Lecture : {esc(lecture_text)}",
        f"👨‍🏫 Teacher: {esc(teacher_text)}",
    ]
    if start_text:
        lines.append(f"🕒 Start   : {esc(start_text)}")
    lines.extend([
        "",
        f"🎥 Quality : {esc(quality_text)}",
        f"⚡ Bot     : @{esc(bot_name)}",
    ])
    return "\n".join(lines)

def fetch_thumbnail(lecture):
    thumb_url = None
    if getattr(lecture, "videoDetails", None) and getattr(lecture.videoDetails, "image", None):
        thumb_url = lecture.videoDetails.image
    if not thumb_url:
        return None
    try:
        resp = requests.get(thumb_url, timeout=30)
        if resp.status_code != 200:
            return None
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
    except Exception:
        return None


def get_lecture_display_name(lecture):
    name = getattr(lecture, "name", None) or ""
    if not name and getattr(lecture, "videoDetails", None):
        name = getattr(lecture.videoDetails, "name", None) or ""
    if not name and getattr(lecture, "tags", None):
        first_tag = lecture.tags[0] if lecture.tags else None
        name = getattr(first_tag, "name", None) or ""
    if not name:
        name = f"Lecture ID: {getattr(lecture, 'id', '')}"
    return name


def _looks_like_id(value):
    if not value:
        return False
    text = str(value).strip()
    if not text:
        return False
    if text.isdigit() and len(text) >= 8:
        return True
    if len(text) == 24:
        for ch in text:
            if ch not in "0123456789abcdefABCDEF":
                return False
        return True
    return False


def _get_val(obj, key):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _normalize_name(first, last):
    parts = []
    if first:
        parts.append(str(first).strip())
    if last:
        parts.append(str(last).strip())
    text = " ".join([p for p in parts if p])
    return text or None


def _get_name_from_obj(obj):
    if obj is None:
        return None
    first = _get_val(obj, "firstName") or _get_val(obj, "first_name")
    last = _get_val(obj, "lastName") or _get_val(obj, "last_name")
    name = _normalize_name(first, last)
    if name:
        return name
    for key in (
        "name",
        "fullName",
        "displayName",
        "profileName",
        "teacherName",
        "facultyName",
        "instructorName",
    ):
        value = _get_val(obj, key)
        if value:
            return str(value)
    return None


def _get_id_from_obj(obj):
    if obj is None:
        return None
    for key in (
        "id",
        "_id",
        "teacherId",
        "teacher_id",
        "userId",
        "user_id",
        "facultyId",
        "faculty_id",
        "instructorId",
        "instructor_id",
    ):
        value = _get_val(obj, key)
        if value:
            return str(value)

def _get_lecture_cache_id(lecture):
    lec_id = getattr(lecture, "id", None)
    if lec_id:
        return str(lec_id)
    video_details = getattr(lecture, "videoDetails", None)
    video_id = getattr(video_details, "id", None) if video_details else None
    if video_id:
        return str(video_id)
    return None
    return None


def extract_teacher_metadata(lecture):
    teacher_ids = []
    teacher_names = []
    fallback_names = []

    def _add_id(value):
        if value and value not in teacher_ids:
            teacher_ids.append(value)

    def _add_name(value):
        if value and value not in teacher_names:
            teacher_names.append(value)

    def _process_teacher_obj(teacher_obj):
        if teacher_obj is None:
            return
        if isinstance(teacher_obj, str):
            if _looks_like_id(teacher_obj):
                _add_id(teacher_obj)
            else:
                _add_name(teacher_obj)
            return

        t_id = _get_id_from_obj(teacher_obj)
        t_name = _get_name_from_obj(teacher_obj)

        if not t_name:
            for nest_key in ("user", "profile", "teacher", "instructor", "author", "faculty"):
                nested = _get_val(teacher_obj, nest_key)
                if isinstance(nested, (list, tuple)):
                    for item in nested:
                        _add_id(_get_id_from_obj(item))
                        _add_name(_get_name_from_obj(item))
                else:
                    t_name = _get_name_from_obj(nested) or t_name
                    t_id = _get_id_from_obj(nested) or t_id

        _add_id(t_id)
        _add_name(t_name)

    # Collect any obvious name/id fields from the lecture object
    for attr in (
        "teacherName",
        "teacherNames",
        "facultyName",
        "facultyNames",
        "instructorName",
        "instructorNames",
    ):
        value = _get_val(lecture, attr)
        if value:
            if isinstance(value, (list, tuple)):
                for v in value:
                    fallback_names.append(str(v))
            else:
                fallback_names.append(str(value))

    for attr in (
        "teacherId",
        "teacherIds",
        "facultyId",
        "facultyIds",
        "instructorId",
        "instructorIds",
        "userId",
        "userIds",
    ):
        value = _get_val(lecture, attr)
        if value:
            if isinstance(value, (list, tuple)):
                for v in value:
                    _add_id(str(v))
            else:
                _add_id(str(value))

    video_details = _get_val(lecture, "videoDetails")
    if video_details:
        for attr in (
            "teacherName",
            "teacherNames",
            "facultyName",
            "facultyNames",
            "instructorName",
            "instructorNames",
        ):
            value = _get_val(video_details, attr)
            if value:
                if isinstance(value, (list, tuple)):
                    for v in value:
                        fallback_names.append(str(v))
                else:
                    fallback_names.append(str(value))
        for attr in ("teacherId", "teacherIds", "facultyId", "facultyIds", "instructorId", "instructorIds"):
            value = _get_val(video_details, attr)
            if value:
                if isinstance(value, (list, tuple)):
                    for v in value:
                        _add_id(str(v))
                else:
                    _add_id(str(value))

    # Process structured teachers list if present
    teachers_value = (
        _get_val(lecture, "teachers")
        or _get_val(lecture, "teacher")
        or _get_val(lecture, "instructors")
        or _get_val(lecture, "faculty")
    )
    if isinstance(teachers_value, (list, tuple)):
        for teacher in teachers_value:
            _process_teacher_obj(teacher)
    elif teachers_value is not None:
        _process_teacher_obj(teachers_value)

    # Fallback names from earlier discovered simple attributes
    for name in fallback_names:
        _add_name(name)

    return teacher_ids, teacher_names


def _format_eta(seconds_left):
    if seconds_left is None:
        return "--:--"
    seconds_left = int(max(seconds_left, 0))
    hours, rem = divmod(seconds_left, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _progress_bar(pct, width=20):
    filled = int(round((pct / 100) * width))
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def _make_rich_upload_progress(title):
    progress = Progress(
        TextColumn("[bold]Upload[/bold] {task.fields[title]}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.fields[sent]} / {task.fields[total_label]}"),
        expand=True,
    )
    progress.start()
    task_id = progress.add_task("upload", total=1, title=title or "file", sent="0.0 MB", total_label="0.0 MB")
    return progress, task_id


def make_upload_progress_callback(batch_id, lecture_id, server_id, title=None):
    state = {"last_ts": 0.0, "last_pct": -1, "last_bytes": 0, "rich": None}
    use_rich = Progress is not None and _get_env_bool("UPLOAD_PROGRESS_UI", True)
    if use_rich:
        display_title = title or "upload"
        if isinstance(display_title, str) and len(display_title) > 80:
            display_title = f"...{display_title[-77:]}"
        state["rich"] = _make_rich_upload_progress(display_title)

    def _cb(current, total):
        if not total:
            return
        pct = int((current / total) * 100)
        now = time.monotonic()
        if pct == state["last_pct"] and (now - state["last_ts"]) < 0.5:
            return
        elapsed = now - state["last_ts"] if state["last_ts"] else None
        speed_bps = None
        if elapsed and elapsed > 0:
            speed_bps = (current - state["last_bytes"]) / elapsed
        state["last_pct"] = pct
        state["last_ts"] = now
        state["last_bytes"] = current

        sent_mb = current / (1024 * 1024)
        total_mb = total / (1024 * 1024)
        speed_mb = (speed_bps / (1024 * 1024)) if speed_bps and speed_bps > 0 else 0.0
        eta = None
        if speed_bps and speed_bps > 0:
            eta = (total - current) / speed_bps

        if state["rich"]:
            progress, task_id = state["rich"]
            progress.update(
                task_id,
                completed=current,
                total=total,
                sent=f"{sent_mb:.1f} MB",
                total_label=f"{total_mb:.1f} MB",
            )
            if current >= total:
                progress.stop()
                state["rich"] = None
        else:
            bar = _progress_bar(pct)
            sys.stdout.write(
                f"\r⬆️ Uploading {bar} {pct:3d}% | {speed_mb:.2f} MB/s | ETA {_format_eta(eta)} | {sent_mb:.1f}/{total_mb:.1f} MB"
            )
            sys.stdout.flush()
        if db_logger:
            db_logger.mark_progress(batch_id, lecture_id, int(current), int(total), pct, server_id)

    return _cb

def _pyrogram_upload(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False):
    try:
        from mainLogic.utils.telegram_uploader import upload as pyrogram_upload
    except Exception as e:
        return {"ok": False, "error": f"pyrogram importer failed: {e}"}
    try:
        result = pyrogram_upload(
            file_path,
            caption=caption,
            as_video=as_video,
            progress_callback=progress_callback,
            thumb_path=thumb_path,
            progress_message=progress_message,
            progress_meta={"title": os.path.basename(file_path) if file_path else 'file'},
        )
        return {
            "ok": True,
            "result": {
                "chat": {"id": result.get("chat_id")},
                "message_id": result.get("message_id"),
                "file_id": result.get("file_id"),
                "file_type": result.get("file_type"),
            },
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def upload_to_telegram(file_path, caption=None, thumb_path=None, as_video=False, progress_callback=None):
    """Try a faster MTProto backend first (if configured), then fall back to Pyrogram bot upload."""
    is_video_ext = isinstance(file_path, str) and file_path.lower().endswith(
        (".mp4", ".mkv", ".webm", ".mov", ".avi")
    )
    as_video_effective = as_video or is_video_ext

    preferred = os.environ.get("TELEGRAM_PREFERRED_BACKEND", "auto").lower()
    try_mtproto = False
    if preferred == 'auto' or 'telethon' in preferred or 'tdlib' in preferred or 'tdlight' in preferred:
        try_mtproto = True

    # Attempt Telethon MTProto (via mtproto_batch_upload) first when allowed
    if try_mtproto:
        try:
            from mainLogic.utils.telegram_uploader import mtproto_batch_upload

            chat_id = os.environ.get("TELEGRAM_CHAT_ID")
            session = os.environ.get("TELETHON_SESSION", "telethon_session")
            # Allow configuring MTProto concurrency via env `TELEGRAM_UPLOAD_WORKERS` (falls back to 4)
            try:
                _mt_conc = int(os.environ.get("TELEGRAM_UPLOAD_WORKERS", "4"))
            except Exception:
                _mt_conc = 4
            results = asyncio.run(mtproto_batch_upload([file_path], chat_id=chat_id, session_name=session, concurrency=_mt_conc, caption=caption, thumb_path=thumb_path, as_video=upload_as_video, progress_callback=progress_callback))
            if results and isinstance(results, list) and results[0].get('ok'):
                r = results[0]
                return {
                    "method": "telethon",
                    "status_code": 200,
                    "data": {"ok": True, "result": {"chat": {"id": r.get('chat_id')}, "message_id": r.get('message_id')}},
                }
        except Exception as e:
            print(f"[VID_DL] mtproto attempt failed: {e}")

    # Fallback: use Pyrogram bot upload (existing path)
    pyrogram_resp = _pyrogram_upload(
        file_path,
        caption=caption,
        as_video=as_video_effective,
        progress_callback=progress_callback,
        thumb_path=thumb_path,
        progress_message=False,  # No message progress, use callback only
    )
    if pyrogram_resp.get("ok"):
        return {
            "method": "pyrogram",
            "status_code": 200,
            "data": {"ok": True, "result": pyrogram_resp.get("result")},
        }
    raise RuntimeError(f"Upload failed: {pyrogram_resp.get('error')}")

def process_lecture_download_upload(lecture, lecture_name, subject_slug, subject_name):
    if not download_enabled:
        return
    if not ensure_free_space(base_download_directory, min_free_gb):
        debugger.error("Stopping due to low disk space.")
        raise RuntimeError("low_disk_space")

    teacher_ids, teacher_names = extract_teacher_metadata(lecture)
    if subject_slug in subject_teacher_map:
        subj_teachers = subject_teacher_map.get(subject_slug) or {}
        id_to_name = subj_teachers.get("id_to_name") or {}
        for t_id in (subj_teachers.get("ids") or []):
            if t_id not in teacher_ids:
                teacher_ids.append(t_id)
        for t_id in teacher_ids:
            t_name = id_to_name.get(t_id)
            if t_name and t_name not in teacher_names:
                teacher_names.append(t_name)
        if not teacher_names:
            for t_name in (subj_teachers.get("names") or []):
                if t_name not in teacher_names:
                    teacher_names.append(t_name)
    teacher_ids_text = ", ".join(teacher_ids) if teacher_ids else None
    teacher_names_text = ", ".join(teacher_names) if teacher_names else None
    lecture_cache_id = _get_lecture_cache_id(lecture)

    debugger.info(f"  Lecture teachers attr: {getattr(lecture, 'teachers', None)}")
    debugger.info(f"  Extracted teacher IDs: {teacher_ids_text}, Names: {teacher_names_text}")

    # If no teacher names were found but we have teacher IDs, try fetching
    # richer lecture data from the Batch API to resolve teacher names.
    if (not teacher_names or len(teacher_names) == 0) and teacher_ids:
        try:
            # batch_slug is the user-provided batch slug in scope
            lec_detail = None
            try:
                lec_detail = batch_api.get_batch_lectures(lecture.id, batch_slug)
            except Exception:
                # Some API wrappers expect (lecture_id, batch_name) ordering or different method
                try:
                    lec_detail = batch_api.get_batch_lectures(lecture.id, batch_name=batch_slug)
                except Exception:
                    lec_detail = None
            if lec_detail:
                debugger.info(f"  API lec_detail: {lec_detail}")
                detail_obj = lec_detail
                if isinstance(lec_detail, dict) and lec_detail.get("data"):
                    detail_obj = lec_detail.get("data")
                elif getattr(lec_detail, "data", None) is not None:
                    detail_obj = getattr(lec_detail, "data")

                detail_ids, detail_names = extract_teacher_metadata(detail_obj)
                if detail_ids or detail_names:
                    debugger.info(f"  Candidate teachers from API: ids={detail_ids} names={detail_names}")
                    for t_id in detail_ids:
                        if t_id not in teacher_ids:
                            teacher_ids.append(t_id)
                    for t_name in detail_names:
                        if t_name not in teacher_names:
                            teacher_names.append(t_name)
                teacher_names_text = ", ".join(teacher_names) if teacher_names else None
                debugger.info(f"  Updated teacher names after API: {teacher_names_text}")
        except Exception as e:
            debugger.debug(f"Teacher name resolution via API failed: {e}")

    if db_logger:
        if not force_reupload and db_logger.is_upload_done(batch_id, lecture.id):
            debugger.info("  Skipping: already uploaded (DB shows done).")
            return
    elif lecture_cache_id and not force_reupload and _local_is_upload_done(batch_id, lecture_cache_id):
        debugger.info("  Skipping: already uploaded (local cache).")
        return
        course_row_id = db_logger.upsert_course(
            batch_id=batch_id,
            batch_slug=batch_slug,
            course_name=batch_display_name,
        )
        subject_row_id = db_logger.upsert_subject(
            course_id=course_row_id,
            subject_slug=subject_slug,
            subject_name=subject_name,
        )
        chapter_row_id = db_logger.upsert_chapter(
            subject_id=subject_row_id,
            chapter_name=getattr(lecture, "chapter_name", None),
        )
        db_logger.upsert_lecture(
            batch_id=batch_id,
            lecture_id=lecture.id,
            subject_slug=subject_slug,
            subject_name=subject_name,
            chapter_name=getattr(lecture, "chapter_name", None),
            lecture_name=lecture_name,
            start_time=str(lecture.startTime) if lecture.startTime else None,
            course_id=course_row_id,
            subject_id=subject_row_id,
            chapter_id=chapter_row_id,
            display_order=getattr(lecture, 'display_order', None),
            chapter_total=getattr(lecture, 'chapter_total', None),
        )
        if teacher_ids or teacher_names:
            max_len = max(len(teacher_ids), len(teacher_names))
            for idx in range(max_len):
                t_id = teacher_ids[idx] if idx < len(teacher_ids) else None
                t_name = teacher_names[idx] if idx < len(teacher_names) else None
                teacher_row_id = db_logger.upsert_teacher(teacher_id=t_id, teacher_name=t_name)
                db_logger.link_lecture_teacher(batch_id, lecture.id, teacher_row_id)
        can_process = db_logger.reserve_lecture(
            batch_id=batch_id,
            lecture_id=lecture.id,
            subject_slug=subject_slug,
            subject_name=subject_name,
            chapter_name=getattr(lecture, 'chapter_name', None),
            lecture_name=lecture_name,
            start_time=str(lecture.startTime) if lecture.startTime else None,
            server_id=server_id,
            lock_ttl_min=lock_ttl_min,
            batch_slug=batch_slug,
            course_name=batch_display_name,
            teacher_ids=teacher_ids_text,
            teacher_names=teacher_names_text,
        )
        if not can_process:
            debugger.info("  Skipping: already processed or in progress on another server.")
            return

    chapter_name = getattr(lecture, "chapter_name", None)
    name_parts = [subject_name, chapter_name, lecture_name]
    raw_name = "_".join([part for part in name_parts if part])
    if not raw_name:
        raw_name = lecture_name or str(lecture.id)
    download_name = generate_safe_folder_name(raw_name)[:200]
    # If a previous run left a downloaded file, reuse it instead of re-downloading.
    file_path = None
    try:
        if db_logger:
            try:
                recorded = db_logger.get_recorded_file_path(batch_id, lecture.id)
            except Exception:
                recorded = None
            if recorded and os.path.exists(recorded):
                file_path = recorded
                debugger.info(f"  Reusing existing downloaded file from DB: {file_path}")
            else:
                # Fallback: look for files on disk matching download_name
                matches = []
                for ext in (".mp4", ".mkv", ".webm", ".mov", ".avi"):
                    matches.extend(glob.glob(os.path.join(base_download_directory, f"{download_name}*{ext}")))
                if matches:
                    file_path = max(matches, key=os.path.getmtime)
                    debugger.info(f"  Reusing existing downloaded file found locally: {file_path}")
    except Exception as e:
        debugger.warning(f"  Existing-file check failed: {e}")

    if file_path is None:
        try:
            downloaded = downloader(
                name=download_name,
                batch_name=batch_id,
                id=lecture.id,
                directory=base_download_directory,
            )
        except SystemExit as e:
            if db_logger:
                db_logger.mark_status(batch_id, lecture.id, "failed", error=f"download exit {e.code}")
            return
        except Exception as e:
            if db_logger:
                db_logger.mark_status(batch_id, lecture.id, "failed", error=str(e))
            return

        if isinstance(downloaded, str) and os.path.exists(downloaded):
            file_path = downloaded
        else:
            mp4s = glob.glob(os.path.join(base_download_directory, "*.mp4"))
            if mp4s:
                file_path = max(mp4s, key=os.path.getmtime)
    else:
        # We found an existing file; record it in DB so progress and final status include path.
        if db_logger:
            try:
                db_logger.mark_status(batch_id, lecture.id, "downloading", file_path=file_path)
            except Exception:
                pass
    

    # At this point `file_path` is set either from reuse detection or from the downloader result.

    if upload_telegram:
        if db_logger:
            db_logger.mark_status(batch_id, lecture.id, "uploading", file_path=file_path)
        try:
            if not file_path:
                raise RuntimeError("Downloaded file not found for upload")

            teacher_names = None
            if teacher_names_text:
                teacher_names = teacher_names_text
            caption_payload = db_logger.get_caption_payload(batch_id, lecture.id) if db_logger else None
            if caption_payload:
                caption = build_caption(
                    batch_slug,
                    caption_payload.get("subject_slug") or subject_slug,
                    caption_payload.get("subject_name") or subject_name,
                    caption_payload.get("chapter_name") or getattr(lecture, "chapter_name", ""),
                    caption_payload.get("lecture_name") or lecture_name,
                    lecture.id,
                    caption_payload.get("start_time") or lecture.startTime,
                    teacher_names=caption_payload.get("teacher_names") or teacher_names,
                    course_name=caption_payload.get("course_name") or batch_display_name,
                )
            else:
                caption = build_caption(
                    batch_slug,
                    subject_slug,
                    subject_name,
                    getattr(lecture, "chapter_name", ""),
                    lecture_name,
                    lecture.id,
                    lecture.startTime,
                    teacher_names=teacher_names,
                    course_name=batch_display_name,
                )
            thumb_path = fetch_thumbnail(lecture)
            progress_cb = make_upload_progress_callback(
                batch_id,
                lecture.id,
                server_id,
                title=os.path.basename(file_path) if file_path else lecture_name,
            )
            # Ensure file has content before uploading
            if os.path.getsize(file_path) == 0:
                debugger.error(f"  Downloaded file is empty, skipping upload: {file_path}")
                if db_logger:
                    db_logger.mark_status(batch_id, lecture.id, "failed", error="empty_file")
                return
            try:
                upload_result = upload_to_telegram(
                    file_path,
                    caption=caption,
                    thumb_path=thumb_path,
                    as_video=upload_as_video,
                    progress_callback=progress_cb,
                )
            finally:
                sys.stdout.write("\n")
                sys.stdout.flush()
            data = upload_result.get("data") or {}
            if upload_result.get("status_code") == 200 and data.get("ok"):
                result = data.get("result", {})
                file_id = result.get("file_id")
                if not file_id:
                    if upload_result.get("method") == "sendDocument":
                        file_id = (result.get("document") or {}).get("file_id")
                    else:
                        file_id = (result.get("video") or {}).get("file_id")
                # Ensure DB is updated to 'done' before deleting local file.
                db_marked_ok = False
                if db_logger:
                    try:
                        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None
                        db_logger.mark_status(
                            batch_id,
                            lecture.id,
                            "done",
                            file_path=file_path,
                            file_size=file_size,
                            telegram_chat_id=str(result.get("chat", {}).get("id")) if result else None,
                            telegram_message_id=str(result.get("message_id")) if result else None,
                            telegram_file_id=str(file_id) if file_id else None,
                        )
                        db_marked_ok = True
                    except Exception as e:
                        debugger.error(f"  DB mark_status failed after upload; not deleting file: {e}")
                else:
                    # No DB logger configured; treat upload success as sufficient to allow deletion
                    db_marked_ok = True

                if delete_after_upload and file_path and os.path.exists(file_path):
                    if not db_marked_ok:
                        debugger.warning("  Skipping local file deletion because DB update failed.")
                    else:
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            debugger.error(f"  Failed to delete file after upload: {e}")
                        else:
                            # After deletion, try to clear file_path/file_size in DB so records reflect the removal.
                            if db_logger:
                                try:
                                    db_logger.mark_status(
                                        batch_id,
                                        lecture.id,
                                        "done",
                                        file_path=None,
                                        file_size=None,
                                        telegram_chat_id=str(result.get("chat", {}).get("id")) if result else None,
                                        telegram_message_id=str(result.get("message_id")) if result else None,
                                        telegram_file_id=str(file_id) if file_id else None,
                                    )
                                except Exception as e:
                                    debugger.error(f"  Failed to update DB after deleting local file: {e}")
                if lecture_cache_id:
                    _local_mark_upload_done(
                        batch_id,
                        lecture_cache_id,
                        lecture_name=lecture_name,
                        chapter_name=getattr(lecture, "chapter_name", None),
                    )
                debugger.info(f"  Uploaded to Telegram: {file_path}")
            else:
                error_text = data.get("description") if isinstance(data, dict) else str(data)
                if db_logger:
                    db_logger.mark_status(batch_id, lecture.id, "failed", error=error_text, file_path=file_path)
                debugger.error(f"  Telegram upload failed: {error_text}")
                # If configured to delete after upload, remove the local file to avoid filling storage
                if delete_after_upload and file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        debugger.error(f"  Failed to delete file after failed upload: {e}")
                    else:
                        debugger.info(f"  Deleted local file after failed upload: {file_path}")
                        if db_logger:
                            try:
                                db_logger.mark_status(batch_id, lecture.id, "failed", file_path=None, file_size=None)
                            except Exception as e:
                                debugger.error(f"  Failed to clear file_path in DB after deletion: {e}")
        except Exception as e:
            err_text = str(e)
            is_flood_wait = "FLOOD_WAIT" in err_text or "FloodWait" in err_text
            if db_logger:
                status = "pending" if is_flood_wait else "failed"
                db_logger.mark_status(batch_id, lecture.id, status, error=err_text, file_path=file_path)
            debugger.error(f"  Telegram upload failed: {e}")
            # If FloodWait, keep file for retry; otherwise delete if configured
            if delete_after_upload and file_path and os.path.exists(file_path) and not is_flood_wait:
                try:
                    os.remove(file_path)
                except Exception as ex_del:
                    debugger.error(f"  Failed to delete file after upload exception: {ex_del}")
                else:
                    debugger.info(f"  Deleted local file after upload exception: {file_path}")
                    if db_logger:
                        try:
                            db_logger.mark_status(batch_id, lecture.id, "failed", file_path=None, file_size=None)
                        except Exception as e:
                            debugger.error(f"  Failed to clear file_path in DB after deletion: {e}")
    else:
        if db_logger:
            try:
                file_size = os.path.getsize(file_path) if file_path and os.path.exists(file_path) else None
                db_logger.mark_status(batch_id, lecture.id, "done", file_path=file_path, file_size=file_size)
                db_marked_ok = True
            except Exception as e:
                debugger.error(f"  DB mark_status failed when marking done: {e}")
                db_marked_ok = False
        else:
            db_marked_ok = True

        # If user requested deletion after upload and DB update succeeded (or no DB), delete file.
        if delete_after_upload and file_path and os.path.exists(file_path):
            if not db_marked_ok:
                debugger.warning("  Skipping local file deletion because DB update failed.")
            else:
                try:
                    os.remove(file_path)
                except Exception as e:
                    debugger.error(f"  Failed to delete local file after marking done: {e}")
                else:
                    if db_logger:
                        try:
                            db_logger.mark_status(batch_id, lecture.id, "done", file_path=None, file_size=None)
                        except Exception as e:
                            debugger.error(f"  Failed to clear file_path in DB after deletion: {e}")

if purchased_batches:
    for batch in purchased_batches:
        if batch.get("slug") == batch_input or batch.get("_id") == batch_input:
            batch_slug = batch.get("slug") or batch_input
            batch_id = batch.get("_id")
            batch_display_name = batch.get("name")
            break

if not batch_id:
    debugger.error("Batch ID not found for the provided slug/id. Use --list-batches to confirm.")
    exit()

all_subjects = batch_api.get_batch_details(batch_name=batch_slug)
subject_name_map = {}
subject_teacher_map = {}
for subj in all_subjects or []:
    sslug = getattr(subj, "slug", None)
    sname = (
        getattr(subj, "name", None)
        or getattr(subj, "title", None)
        or getattr(subj, "subject", None)
        or (subj.get("subject") if isinstance(subj, dict) else None)
    )
    if sslug:
        subject_name_map[sslug] = sname or sslug
    if sslug:
        teacher_entries = (
            getattr(subj, "teacherIds", None)
            or getattr(subj, "teacher_ids", None)
            or (subj.get("teacherIds") if isinstance(subj, dict) else None)
            or []
        )
        t_ids = []
        t_names = []
        id_to_name = {}
        for entry in teacher_entries:
            if isinstance(entry, str):
                if _looks_like_id(entry) and entry not in t_ids:
                    t_ids.append(entry)
                continue
            t_id = _get_id_from_obj(entry)
            t_name = _get_name_from_obj(entry)
            if t_id and t_id not in t_ids:
                t_ids.append(t_id)
            if t_name and t_name not in t_names:
                t_names.append(t_name)
            if t_id and t_name:
                id_to_name[t_id] = t_name
        if not t_names and id_to_name:
            t_names.extend(list(id_to_name.values()))
        if t_ids or t_names:
            subject_teacher_map[sslug] = {"ids": t_ids, "names": t_names, "id_to_name": id_to_name}

# Interactive selection beyond batch
selection_map = {}
print("Available subjects:")
for idx, subj in enumerate(all_subjects, start=1):
    print(f"{idx}. {getattr(subj,'slug', '')}")
subj_choice = input("Enter subject number(s) comma-separated, or 'all' to pick all (required): ").strip()
if not subj_choice:
    debugger.error("No subject selected. Use 'all' to select all subjects.")
    exit()
elif subj_choice.lower() == 'all':
    subjects_to_select = all_subjects
    interactive_auto_all = True
else:
    interactive_auto_all = False
    subjects_to_select = []
    for part in subj_choice.split(','):
        try:
            i = int(part.strip()) - 1
            if 0 <= i < len(all_subjects):
                subjects_to_select.append(all_subjects[i])
        except ValueError:
            continue

for subj in subjects_to_select:
    sslug = getattr(subj, 'slug', None)
    if not sslug:
        continue
    selection_map[sslug] = {}
    chapters = batch_api.get_batch_subjects(batch_name=batch_slug, subject_name=sslug)
    if not chapters:
        continue
    print(f"\nChapters for subject {sslug}:")
    for cidx, ch in enumerate(chapters, start=1):
        print(f"{cidx}. {getattr(ch,'name','')} (videos={getattr(ch,'videos',0)})")
    ch_choice = input("Enter chapter number(s) comma-separated, or 'all' to pick all chapters: ").strip()
    if interactive_auto_all or (not ch_choice) or ch_choice.lower() == 'all':
        chosen_chapters = [ch for ch in chapters if getattr(ch,'videos',0) > 0]
    else:
        chosen_chapters = []
        for part in ch_choice.split(','):
            try:
                ci = int(part.strip()) - 1
                if 0 <= ci < len(chapters):
                    if getattr(chapters[ci],'videos',0) > 0:
                        chosen_chapters.append(chapters[ci])
            except ValueError:
                continue

    selection_map[sslug]['chapters'] = {}
    for chapter in chosen_chapters:
        ch_name = chapter.name
        print(f"\nLectures in chapter: {ch_name}")
        lectures = _fetch_chapter_lectures(
            batch_api,
            batch_id,
            batch_slug,
            subj,
            sslug,
            chapter,
        )
        if not lectures:
            print("  (no lectures)")
            continue
        for lidx, lec in enumerate(lectures, start=1):
            title = getattr(lec,'name', None) or (lec.id if hasattr(lec,'id') else '<no-id>')
            tstr = getattr(lec,'startTime', None)
            if tstr:
                tdisp = tstr.strftime('%d-%m-%Y %H:%M')
            else:
                tdisp = 'unknown'
            print(f"  {lidx}. {title} | {tdisp} | id={getattr(lec,'id','')}")
        if interactive_auto_all:
            selection_map[sslug]['chapters'][ch_name] = {'all': True}
        else:
            allq = input("Download ALL lectures from this chapter? (y/N): ").strip().lower()
            if allq == 'y':
                selection_map[sslug]['chapters'][ch_name] = {'all': True}
            else:
                nums = input("Enter lecture number(s) comma-separated to download, or press Enter to skip: ").strip()
                chosen_ids = []
                if nums:
                    for part in nums.split(','):
                        try:
                            li = int(part.strip()) - 1
                            if 0 <= li < len(lectures):
                                chosen_ids.append(lectures[li].id)
                        except ValueError:
                            continue
                if chosen_ids:
                    selection_map[sslug]['chapters'][ch_name] = {'all': False, 'ids': chosen_ids}


# Define UTC timezone for consistency
UTC = timezone.utc # Python 3.2+ recommended way for UTC

# Decide which subjects to iterate (interactive selection may limit this)
if selection_map:
    subjects_iter = [s for s in all_subjects if getattr(s, 'slug', None) in selection_map]
else:
    subjects_iter = all_subjects

# Iterate through subjects
for subject in subjects_iter:
    # Filter subjects based on preferences (from CLI or prefs file)
    if subjects_of_interest and subject.slug not in subjects_of_interest:
        debugger.info(f"Skipping subject '{subject.slug}' as it's not in subjects of interest.")
        continue

    subject_name = subject_name_map.get(subject.slug, subject.slug)
    if subject_name == subject.slug:
        subject_name = re.sub(r"-[0-9]{4,}$", "", subject_name).replace("_", " ")

    debugger.var(f"Processing subject: {subject.slug}")
    chapters_in_subject = batch_api.get_batch_subjects(batch_name=batch_slug, subject_name=subject.slug)

    all_lectures_in_subject = []
    
    # Iterate through all chapters to collect all relevant lectures
    for chapter in chapters_in_subject:
        if not (chapter.name and chapter.videos > 0):
            continue
        # If interactive selection used, skip chapters not selected
        if subject.slug in selection_map:
            selected_chapters = selection_map[subject.slug].get('chapters', {})
            if chapter.name not in selected_chapters:
                continue
        lectures_in_chapter = _fetch_chapter_lectures(
            batch_api,
            batch_id,
            batch_slug,
            subject,
            subject.slug,
            chapter,
        )
        # Add chapter_name to each lecture object for downloader context
        if subject.slug in selection_map:
            chap_sel = selection_map[subject.slug]['chapters'].get(chapter.name, {})
            if chap_sel.get('all'):
                for idx, lecture in enumerate(lectures_in_chapter, start=1):
                    lecture.chapter_name = chapter.name
                    lecture.display_order = idx
                    lecture.chapter_total = len(lectures_in_chapter)
                    all_lectures_in_subject.append(lecture)
            else:
                allowed_ids = set(chap_sel.get('ids', []))
                for idx, lecture in enumerate(lectures_in_chapter, start=1):
                    if lecture.id in allowed_ids:
                        lecture.chapter_name = chapter.name
                        lecture.display_order = idx
                        lecture.chapter_total = len(lectures_in_chapter)
                        all_lectures_in_subject.append(lecture)
        else:
            for idx, lecture in enumerate(lectures_in_chapter, start=1):
                lecture.chapter_name = chapter.name
                lecture.display_order = idx
                lecture.chapter_total = len(lectures_in_chapter)
                all_lectures_in_subject.append(lecture)

    # Sort lectures by start time in descending order (latest first)
    sorted_lectures = sorted(all_lectures_in_subject, key=attrgetter('startTime'), reverse=True)

    if not sorted_lectures:
        debugger.warning(f"No lectures found for subject: {subject.slug}.")
        continue

    for lecture in sorted_lectures:
        debugger.info(f"Processing lecture for subject '{subject.slug}':")
        lecture_name = get_lecture_display_name(lecture)
        debugger.info(f"  Name: {lecture_name}")
        if lecture.startTime:
            debugger.info(f"  Start Time: {lecture.startTime.strftime('%d-%m-%Y %H:%M:%S %Z')}")
        debugger.info(f"  Batch Name: {batch_slug}")
        debugger.info(f"  Lecture ID: {lecture.id}")
        try:
            process_lecture_download_upload(lecture, lecture_name, subject.slug, subject_name)
        except RuntimeError:
            break