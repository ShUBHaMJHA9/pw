import argparse
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

import pytz # Make sure to pip install pytz if you haven't already
import requests
from beta.batch_scraper_2.module import ScraperModule

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

batch_name_default = "yakeen-neet-2-0-2026-854543"

# --- 1. Set up argparse ---
parser = argparse.ArgumentParser(description="Scrape and download lectures from PenPencil batches.")
parser.add_argument(
    "--batch", "-b",
    type=str,
    default=batch_name_default,
    help=f"Name/slug of the batch to scrape (default: {batch_name_default})."
)
parser.add_argument(
    "--subjects", "-s",
    type=str,
    help="Comma-separated list of subject slugs to filter. If not provided, all subjects are processed.",
)
parser.add_argument(
    "--subject",
    type=str,
    help="Single subject slug to filter. Overrides --subjects.",
)
parser.add_argument(
    "--chapter",
    type=str,
    help="Chapter name to filter within the selected subject.",
)
parser.add_argument(
    "--lecture-id",
    type=str,
    help="Download a specific lecture ID (requires --subject and --chapter).",
)
parser.add_argument(
    "--latest-nth", "-n",
    type=int,
    default=None, # No default, means get all latest for each subject by default
    help="Fetch the Nth latest lecture for each subject (0 for the absolute latest, 1 for the second latest, etc.). "
         "If not provided, all lectures (matching --date if set) are considered.",
)
parser.add_argument(
    "--date", "-dt",
    type=str,
    default=None,
    help="Filter lectures to a specific date in 'dd.mm.yyyy' format. "
         "If used with -n, it finds the Nth latest lecture *on that specific date*.",
)
parser.add_argument(
    "--download", "-d",
    action="store_true",
    help="Enable downloading of the selected lecture(s)."
)
parser.add_argument(
    "--all-lectures",
    action="store_true",
    help="Download all lectures for selected subjects/chapters."
)
parser.add_argument(
    "--download-dir", "-dd",
    type=str,
    default="./" if not ScraperModule.prefs.get("extension_download_dir") else ScraperModule.prefs.get("extension_download_dir"),
    help="Base directory for downloads (default: ./)."
)
parser.add_argument(
    "--list-batches",
    action="store_true",
    help="List purchased batches and exit."
)
parser.add_argument(
    "--select",
    action="store_true",
    help="Interactive selection of batch/subject/lecture."
)
parser.add_argument(
    "--db-log",
    action="store_true",
    help="Enable DB logging (attempts to import mysql logger)."
)

parser.add_argument(
    "--upload-telegram",
    action="store_true",
    help="Upload downloaded lecture to Telegram using bot credentials from .env."
)
parser.add_argument(
    "--upload-as-video",
    action="store_true",
    help="Upload as Telegram video instead of document (falls back to document on failure)."
)

parser.add_argument(
    "--db-url",
    type=str,
    default=None,
    help="Override DB URL (defaults to PWDL_DB_URL from .env)."
)
parser.add_argument(
    "--server-id",
    type=str,
    default=None,
    help="Unique server identifier for DB locks (defaults to hostname)."
)
parser.add_argument(
    "--min-free-gb",
    type=float,
    default=2.0,
    help="Minimum free disk space (GB) required to start a download."
)
parser.add_argument(
    "--lock-ttl-min",
    type=int,
    default=120,
    help="Minutes before an in-progress DB lock can be reclaimed."
)
parser.add_argument(
    "--delete-after-upload",
    action="store_true",
    help="Delete local file after successful upload."
)
parser.add_argument(
    "--force-reupload",
    action="store_true",
    help="Ignore DB 'done' state and force re-download/re-upload."
)

args = parser.parse_args()

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

prefs = ScraperModule.prefs # Still keep prefs for other potential settings
batch_api = ScraperModule.batch_api
debugger = ScraperModule.debugger

# --- Apply arguments ---
batch_input = args.batch
# Process subjects_of_interest from command line
if args.subject:
    subjects_of_interest = {args.subject.strip()}
elif args.subjects:
    subjects_of_interest = set([s.strip() for s in args.subjects.split(',') if s.strip()])
else:
    # If not provided via CLI, fall back to prefs or keep empty for no filtering
    subjects_of_interest = set(ScraperModule.prefs.get("subjects_of_interest", []))

download_enabled = args.download
upload_telegram = args.upload_telegram
upload_as_video = args.upload_as_video
base_download_directory = args.download_dir
server_id = args.server_id or socket.gethostname()
min_free_gb = args.min_free_gb
lock_ttl_min = args.lock_ttl_min
delete_after_upload = args.delete_after_upload

# Parse the specific date filter if provided
specific_date_filter_utc = None
if args.date:
    try:
        # Assuming input is dd.mm.yyyy, convert to datetime object at midnight in IST, then to UTC
        day, month, year = map(int, args.date.split('.'))
        target_date_ist = datetime(year, month, day, 0, 0, 0, tzinfo=pytz.timezone('Asia/Kolkata'))
        specific_date_filter_utc = target_date_ist.astimezone(timezone.utc)
        debugger.info(f"Filtering for lectures on specific date: {args.date} (UTC: {specific_date_filter_utc.date()})")
    except ValueError as e:
        debugger.error(f"Invalid date format for --date: {args.date}. Please use 'dd.mm.yyyy'. Error: {e}")
        exit() # Exit if the date format is incorrect


# Ensure batch_api is initialized before use
if batch_api is None:
    debugger.error("ScraperModule.batch_api is not initialized. Please check token setup.")
    exit()

if args.list_batches:
    batches = batch_api.get_purchased_batches(all_pages=True)
    if not batches:
        debugger.warning("No purchased batches found.")
        exit()
    for idx, batch in enumerate(batches, start=1):
        name = batch.get("name", "")
        slug = batch.get("slug", "")
        batch_id = batch.get("_id", "")
        debugger.info(f"{idx}. {name} | slug={slug} | id={batch_id}")
    exit()

batch_slug = batch_input
batch_id = None
batch_display_name = None

try:
    purchased_batches = batch_api.get_purchased_batches(all_pages=True)
except Exception as e:
    purchased_batches = []
    debugger.error(f"Failed to load purchased batches: {e}")

# If interactive select is requested, prompt the user to pick a batch
if args.select:
    if not purchased_batches:
        debugger.error("No purchased batches available for selection.")
        exit()
    print("Purchased batches:")
    for idx, b in enumerate(purchased_batches, start=1):
        print(f"{idx}. {b.get('name','')}  (slug={b.get('slug')}, id={b.get('_id')})")
    choice = input("Enter batch number or slug (or press Enter to keep default): ").strip()
    if choice:
        # try number first
        try:
            i = int(choice) - 1
            if 0 <= i < len(purchased_batches):
                sel = purchased_batches[i]
                batch_input = sel.get('slug') or sel.get('_id')
            else:
                debugger.error("Invalid batch number selected.")
                exit()
        except ValueError:
            # treat as slug/id
            batch_input = choice

# If DB logging requested, try to import logger
db_logger = None
if args.db_log:
    try:
        from mainLogic.utils import mysql_logger as db_logger
        debugger.info("DB logger loaded.")
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
        db_logger.init(args.db_url)
        db_logger.ensure_schema()
    except Exception as e:
        debugger.error(f"DB init failed: {e}")
        db_logger = None

def _get_env_text(var_name):
    value = os.getenv(var_name)
    return value.strip() if isinstance(value, str) else value

def _get_env_bool(var_name, default=False):
    value = _get_env_text(var_name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

def load_telegram_config():
    token = _get_env_text("TELEGRAM_BOT_TOKEN")
    chat_id = _get_env_text("TELEGRAM_CHAT_ID")
    bot_name = _get_env_text("TELEGRAM_BOT_NAME") or "YourBotName"
    if not token or not chat_id:
        return None
    return {
        "token": token,
        "chat_id": chat_id,
        "bot_name": bot_name,
    }

telegram_cfg = load_telegram_config() if upload_telegram else None
if upload_telegram and not telegram_cfg:
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
        text = re.sub(r"\s+", " ", text)
        # If slug-like with trailing numeric id, drop the id suffix.
        text = re.sub(r"-[0-9]{4,}$", "", text)
        return text.replace("_", " ")

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

    bot_name = telegram_cfg.get("bot_name") if telegram_cfg else "YourBotName"
    if bot_name.startswith("@"):  # Normalize handle for display
        bot_name = bot_name[1:]
    course_text = clean_display_name(course_name or batch_slug)
    teacher_text = _normalize_teacher_names(teacher_names) or "TBA"
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


def extract_teacher_metadata(lecture):
    teacher_ids = []
    teacher_names = []
    fallback_names = []

    # Collect any obvious name/id fields from the lecture object
    for attr in ("teacherName", "teacher", "facultyName", "instructor", "instructors"):
        value = getattr(lecture, attr, None)
        if value:
            # Could be a list, dict, or scalar
            if isinstance(value, (list, tuple)):
                for v in value:
                    fallback_names.append(str(v))
            else:
                fallback_names.append(str(value))

    video_details = getattr(lecture, "videoDetails", None)
    if video_details:
        for attr in ("teacherName", "teacher", "facultyName", "instructor"):
            value = getattr(video_details, attr, None)
            if value:
                fallback_names.append(str(value))

    # Process structured teachers list if present
    if getattr(lecture, "teachers", None):
        for teacher in lecture.teachers:
            # If teacher is a dict with possible nested structures
            if isinstance(teacher, dict):
                # possible id fields
                t_id = (
                    teacher.get("id")
                    or teacher.get("_id")
                    or teacher.get("teacherId")
                    or teacher.get("userId")
                )
                # possible name fields
                t_name = (
                    teacher.get("name")
                    or teacher.get("fullName")
                    or teacher.get("firstName")
                    or teacher.get("displayName")
                    or teacher.get("profileName")
                )
                # nested user/profile keys
                if not t_name:
                    for nest_key in ("user", "profile", "teacher", "instructor", "author"):
                        nested = teacher.get(nest_key)
                        if isinstance(nested, dict):
                            t_name = (
                                nested.get("name")
                                or nested.get("fullName")
                                or nested.get("displayName")
                                or nested.get("firstName")
                                or nested.get("profileName")
                            )
                            if t_name:
                                break
                if t_id:
                    t_id = str(t_id)
                    if t_id not in teacher_ids:
                        teacher_ids.append(t_id)
                if t_name:
                    t_name = str(t_name)
                    if t_name not in teacher_names:
                        teacher_names.append(t_name)
            elif isinstance(teacher, str):
                # string may be an id or a name
                if _looks_like_id(teacher):
                    if teacher not in teacher_ids:
                        teacher_ids.append(teacher)
                else:
                    if teacher not in teacher_names:
                        teacher_names.append(teacher)

    # Fallback names from earlier discovered simple attributes
    for name in fallback_names:
        if name and name not in teacher_names:
            teacher_names.append(name)

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

def _tg_api_url(method):
    token = telegram_cfg["token"]
    return f"https://api.telegram.org/bot{token}/{method}"

def _tg_send_file(method, file_path, caption=None, thumb_path=None):
    chat_id = telegram_cfg["chat_id"]
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    files = {
        "video" if method == "sendVideo" else "document": open(file_path, "rb")
    }
    if thumb_path and os.path.exists(thumb_path):
        files["thumbnail"] = open(thumb_path, "rb")
    try:
        resp = requests.post(_tg_api_url(method), data=data, files=files, timeout=120)
        return {
            "method": method,
            "status_code": resp.status_code,
            "data": resp.json() if resp.content else {},
        }
    finally:
        for handle in files.values():
            try:
                handle.close()
            except Exception:
                pass

def _telethon_upload(file_path, caption=None, as_video=False, progress_callback=None, thumb_path=None, progress_message=False):
    try:
        from mainLogic.utils import telegram_uploader as telethon_uploader
    except Exception as e:
        return {"ok": False, "error": f"telethon importer failed: {e}"}
    try:
        result = telethon_uploader.upload(
            file_path,
            caption=caption,
            as_video=as_video,
            progress_callback=progress_callback,
            thumb_path=thumb_path,
            progress_message=progress_message,
            progress_meta={"server_id": server_id, "title": os.path.basename(file_path) if file_path else 'file'},
        )
        return {
            "ok": True,
            "result": {
                "chat": {"id": result.get("chat_id")},
                "message_id": result.get("message_id"),
            },
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def upload_to_telegram(file_path, caption=None, thumb_path=None, as_video=False, progress_callback=None):
    is_video_ext = isinstance(file_path, str) and file_path.lower().endswith(
        (".mp4", ".mkv", ".webm", ".mov", ".avi")
    )
    as_video_effective = as_video or is_video_ext
    # Prefer Telethon when available; fall back to Bot API.
    if _get_env_text("TELEGRAM_API_ID") and _get_env_text("TELEGRAM_API_HASH"):
        progress_message = _get_env_bool("TELEGRAM_PROGRESS_UPDATES", False)
        telethon_resp = _telethon_upload(
            file_path,
            caption=caption,
            as_video=as_video_effective,
            progress_callback=progress_callback,
            thumb_path=thumb_path,
            progress_message=progress_message,
        )
        if telethon_resp.get("ok"):
            return {
                "method": "telethon",
                "status_code": 200,
                "data": {"ok": True, "result": telethon_resp.get("result")},
            }
        debugger.warning(f"Telethon upload failed, falling back to Bot API: {telethon_resp.get('error')}")

    primary_method = "sendVideo" if as_video_effective else "sendDocument"
    resp = _tg_send_file(primary_method, file_path, caption=caption, thumb_path=thumb_path)
    data = resp.get("data") or {}
    status_code = resp.get("status_code")
    if as_video_effective and (status_code != 200 or not data.get("ok")):
        description = data.get("description") if isinstance(data, dict) else str(data)
        if isinstance(description, str) and ("file is too big" in description.lower() or "request entity too large" in description.lower()):
            debugger.warning("sendVideo failed due to size. Retrying as document...")
        resp = _tg_send_file("sendDocument", file_path, caption=caption, thumb_path=thumb_path)
    return resp

def process_lecture_download_upload(lecture, lecture_name, subject_slug, subject_name):
    if not download_enabled:
        return
    if not ensure_free_space(base_download_directory, min_free_gb):
        debugger.error("Stopping due to low disk space.")
        raise RuntimeError("low_disk_space")

    teacher_ids, teacher_names = extract_teacher_metadata(lecture)
    teacher_ids_text = ", ".join(teacher_ids) if teacher_ids else None
    teacher_names_text = ", ".join(teacher_names) if teacher_names else None

    if db_logger:
        if not getattr(args, 'force_reupload', False) and db_logger.is_upload_done(batch_id, lecture.id):
            debugger.info("  Skipping: already uploaded (DB shows done).")
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
                                    )
                                except Exception as e:
                                    debugger.error(f"  Failed to update DB after deleting local file: {e}")
                debugger.info(f"  Uploaded to Telegram: {file_path}")
            else:
                error_text = data.get("description") if isinstance(data, dict) else str(data)
                if db_logger:
                    db_logger.mark_status(batch_id, lecture.id, "failed", error=error_text, file_path=file_path)
                debugger.error(f"  Telegram upload failed: {error_text}")
        except Exception as e:
            if db_logger:
                db_logger.mark_status(batch_id, lecture.id, "failed", error=str(e), file_path=file_path)
            debugger.error(f"  Telegram upload failed: {e}")
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
for subj in all_subjects or []:
    sslug = getattr(subj, "slug", None)
    sname = getattr(subj, "name", None) or getattr(subj, "title", None)
    if sslug:
        subject_name_map[sslug] = sname or sslug

# If interactive selection beyond batch was requested, build a selection map
selection_map = {}
if args.select:
    print("Available subjects:")
    for idx, subj in enumerate(all_subjects, start=1):
        print(f"{idx}. {getattr(subj,'slug', '')}")
    subj_choice = input("Enter subject number(s) comma-separated, or 'all' to pick all (required): ").strip()
    if not subj_choice:
        debugger.error("No subject selected. Use 'all' to select all subjects.")
        exit()
    elif subj_choice.lower() == 'all':
        subjects_to_select = all_subjects
    else:
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
        if not ch_choice or ch_choice.lower() == 'all':
            chosen_chapters = [ch.name for ch in chapters if getattr(ch,'videos',0) > 0]
        else:
            chosen_chapters = []
            for part in ch_choice.split(','):
                try:
                    ci = int(part.strip()) - 1
                    if 0 <= ci < len(chapters):
                        if getattr(chapters[ci],'videos',0) > 0:
                            chosen_chapters.append(chapters[ci].name)
                except ValueError:
                    continue
        # for each chosen chapter, ask whether download all or pick lectures
        selection_map[sslug]['chapters'] = {}
        for ch_name in chosen_chapters:
            print(f"\nLectures in chapter: {ch_name}")
            lectures = batch_api.get_batch_chapters(batch_name=batch_slug, subject_name=sslug, chapter_name=ch_name)
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
if args.select and selection_map:
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
        if args.chapter and chapter.name != args.chapter:
            continue
        if not (chapter.name and chapter.videos > 0):
            continue
        # If interactive selection used, skip chapters not selected
        if args.select and subject.slug in selection_map:
            selected_chapters = selection_map[subject.slug].get('chapters', {})
            if chapter.name not in selected_chapters:
                continue
        lectures_in_chapter = batch_api.get_batch_chapters(
            batch_name=batch_slug,
            subject_name=subject.slug,
            chapter_name=chapter.name
        )
        # Add chapter_name to each lecture object for downloader context
        if args.select and subject.slug in selection_map:
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

    # --- Sort and Filter Lectures ---
    filtered_lectures = []

    if specific_date_filter_utc:
        # Filter by specific date first
        target_date_utc_end = specific_date_filter_utc + timedelta(days=1) # Midnight of next day
        for lecture in all_lectures_in_subject:
            if lecture.startTime and \
               specific_date_filter_utc <= lecture.startTime < target_date_utc_end:
                filtered_lectures.append(lecture)
        if not filtered_lectures:
            debugger.warning(f"No lectures found for subject '{subject.slug}' on date {args.date}.")
            continue # Skip to next subject if no lectures found on specified date
    else:
        # If no specific date, consider all collected lectures
        filtered_lectures = all_lectures_in_subject

    # Sort the filtered lectures by start time in descending order (latest first)
    # Using attrgetter is efficient for sorting by an attribute
    sorted_lectures = sorted(filtered_lectures, key=attrgetter('startTime'), reverse=True)

    selected_lecture = None
    if args.lecture_id:
        for lecture in sorted_lectures:
            if lecture.id == args.lecture_id:
                selected_lecture = lecture
                debugger.info(f"Selected lecture ID {args.lecture_id}.")
                break
        if not selected_lecture:
            debugger.warning(f"Lecture ID {args.lecture_id} not found.")
            continue
    elif args.latest_nth is not None:
        if 0 <= args.latest_nth < len(sorted_lectures):
            selected_lecture = sorted_lectures[args.latest_nth]
            debugger.info(f"Selected {args.latest_nth}th latest lecture for subject '{subject.slug}'.")
        else:
            debugger.warning(f"Requested {args.latest_nth}th latest lecture, but only {len(sorted_lectures)} lectures found for subject '{subject.slug}' after date filter (if any).")
            continue # Skip to next subject if nth latest doesn't exist
    elif sorted_lectures and not args.all_lectures: # If no -n is provided, and there are lectures, just pick the very latest
        selected_lecture = sorted_lectures[0]
        debugger.info(f"No --latest-nth specified. Picking the very latest lecture for subject '{subject.slug}'.")

    process_all_selected = bool(args.select and selection_map)

    if process_all_selected and sorted_lectures:
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
    elif args.all_lectures and sorted_lectures:
        for lecture in sorted_lectures:
            debugger.info(f"Processing lecture for subject '{subject.slug}':")
            lecture_name = get_lecture_display_name(lecture)
            debugger.info(f"  Name: {lecture_name}")
            if lecture.startTime:
                debugger.info(f"  Start Time: {lecture.startTime.strftime('%d-%m-%Y %H:%M:%S %Z')}")
            debugger.info(f"  Batch Name: {batch_slug}")
            debugger.info(f"  Lecture ID: {lecture.id}")
            debugger.info(f"  Attempting to download lecture...")
            try:
                process_lecture_download_upload(lecture, lecture_name, subject.slug, subject_name)
            except RuntimeError:
                break
    elif selected_lecture:
        debugger.info(f"Processing selected lecture for subject '{subject.slug}':")
        lecture_name = get_lecture_display_name(selected_lecture)
        debugger.info(f"  Name: {lecture_name}")
        debugger.info(f"  Start Time: {selected_lecture.startTime.strftime('%d-%m-%Y %H:%M:%S %Z')}")
        debugger.info(f"  Batch Name: {batch_slug}")
        debugger.info(f"  Lecture ID: {selected_lecture.id}")
        if download_enabled:
            debugger.info(f"  Attempting to download lecture...")
            try:
                process_lecture_download_upload(selected_lecture, lecture_name, subject.slug, subject_name)
            except RuntimeError:
                pass
        
    else:
        debugger.warning(f"No lectures found for subject: {subject.slug} that match the applied filters.")