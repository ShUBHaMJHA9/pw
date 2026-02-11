#!/usr/bin/env python3
"""
Simple DPP backup tool:
- Finds chapters whose name contains 'dpp' or 'solution'
- For each lecture in those chapters: if a DPP backup exists (DB), skip
  otherwise reuse local file if present, or download, then upload to backup chat
  and record a `dpp_backups` row with metadata.

Usage:
  python tools/dpp_backup.py --batch BATCH_SLUG --backup-chat BACKUP_CHAT_ID --download --upload --db-log

Requires TELEGRAM_* env vars for uploads.
"""
import argparse
import os
import sys
import json
import glob
import time
from pprint import pprint
from datetime import datetime

from beta.batch_scraper_2.module import ScraperModule

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

parser = argparse.ArgumentParser(description="DPP backup helper")
parser.add_argument("--batch", required=True)
parser.add_argument("--backup-chat", help="Telegram chat id to upload backups (overrides env)")
parser.add_argument("--download", action="store_true")
parser.add_argument("--upload", action="store_true")
parser.add_argument("--db-log", action="store_true")
parser.add_argument("--dpp-pdfs", action="store_true", help="Also fetch and backup DPP PDF attachments for matching chapters")
parser.add_argument("--download-all-tests", action="store_true", help="Discover all tests for the batch and download their question images (subject-wise)")
parser.add_argument("--download-dir", default=os.getcwd())
args = parser.parse_args()

batch_slug = args.batch
backup_chat = args.backup_chat or os.getenv("TELEGRAM_BACKUP_CHAT_ID") or os.getenv("PW_BACKUP_CHAT_ID")
download_dir = args.download_dir

batch_api = ScraperModule.batch_api
if batch_api is None:
    print("batch_api not initialized")
    sys.exit(1)

if args.db_log:
    try:
        from mainLogic.utils import mysql_logger as db
        db.init(None)
        db.ensure_schema()
    except Exception as e:
        print("DB logger load failed:", e)
        db = None
else:
    db = None

try:
    from mainLogic.downloader import main as downloader
except Exception:
    downloader = None

try:
    from mainLogic.utils import telegram_uploader as tele_up
except Exception:
    tele_up = None

# Find batch id from purchased batches
purchased = batch_api.get_purchased_batches(all_pages=True)
batch_id = None
batch_display = None
for b in purchased or []:
    if b.get("slug") == batch_slug or b.get("_id") == batch_slug:
        batch_id = b.get("_id")
        batch_display = b.get("name")
        break
if not batch_id:
    print("Batch not found in purchased batches")
    sys.exit(1)

subjects = batch_api.get_batch_details(batch_name=batch_slug) or []

def looks_like_dpp(name):
    if not name:
        return False
    n = name.lower()
    return "dpp" in n or "solution" in n or "solutions" in n


def _serialize(obj):
    """Recursively convert model objects to JSON-serializable primitives."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    if hasattr(obj, "__dict__"):
        return {k: _serialize(v) for k, v in vars(obj).items()}
    try:
        return str(obj)
    except Exception:
        return None

for subject in subjects:
    subj_slug = getattr(subject, "slug", None)
    subj_name = getattr(subject, "name", None) or subj_slug
    chapters = batch_api.get_batch_subjects(batch_name=batch_slug, subject_name=subj_slug)
    if not chapters:
        continue
    for chapter in chapters:
        cname = getattr(chapter, "name", "")
        if not looks_like_dpp(cname):
            continue
        print(f"Processing DPP chapter: {subj_slug} / {cname}")
        lectures = batch_api.get_batch_chapters(batch_name=batch_slug, subject_name=subj_slug, chapter_name=cname)
        for lecture in lectures:
            lec_id = getattr(lecture, "id", None)
            lec_name = getattr(lecture, "name", None) or getattr(getattr(lecture, 'videoDetails', None), 'name', None) or lec_id
            print(" -", lec_id, lec_name)
            # Check DB
            if db:
                existing = db.get_dpp_backup(batch_id, lec_id)
                if existing and existing.get("status") == "done":
                    print("   -> already backed up")
                    continue
            # Try to reuse recorded file path
            file_path = None
            if db:
                try:
                    rec = db.get_recorded_file_path(batch_id, lec_id)
                except Exception:
                    rec = None
                if rec and os.path.exists(rec):
                    file_path = rec
                    print("   -> reusing recorded file", file_path)
            if not file_path:
                # search local dir for matches
                matches = []
                name_pattern = lec_name.replace(' ', '_')[:120]
                for ext in ('.mp4', '.mkv', '.webm', '.mov'):
                    matches.extend(glob.glob(os.path.join(download_dir, f"*{name_pattern}*{ext}")))
                if matches:
                    file_path = max(matches, key=os.path.getmtime)
                    print("   -> reusing local file", file_path)
            if not file_path and args.download:
                if downloader is None:
                    print("   -> downloader unavailable, skipping")
                    if db:
                        db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', status='failed', error='downloader missing')
                    continue
                try:
                    downloaded = downloader(name=lec_name, batch_name=batch_id, id=lec_id, directory=download_dir)
                except SystemExit as e:
                    print("   -> download exit", e)
                    if db:
                        db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', status='failed', error=f"download exit {e.code}")
                    continue
                except Exception as e:
                    print("   -> download failed", e)
                    if db:
                        db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', status='failed', error=str(e))
                    continue
                if isinstance(downloaded, str) and os.path.exists(downloaded):
                    file_path = downloaded
                else:
                    mp4s = glob.glob(os.path.join(download_dir, "*.mp4"))
                    if mp4s:
                        file_path = max(mp4s, key=os.path.getmtime)
            # Upload if requested
            msg_chat = None
            msg_id = None
            if args.upload:
                if not backup_chat:
                    print("   -> no backup chat configured, skipping upload")
                    if db:
                        db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', file_path=file_path, status='skipped', error='no backup chat')
                    continue
                if not file_path or not os.path.exists(file_path):
                    print("   -> no file to upload")
                    if db:
                        db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', file_path=None, status='failed', error='no file')
                    continue
                # temporarily override TELEGRAM_CHAT_ID for uploader
                old = os.environ.get('TELEGRAM_CHAT_ID')
                os.environ['TELEGRAM_CHAT_ID'] = str(backup_chat)
                try:
                    if tele_up is None:
                        print('   -> telegram uploader missing')
                        if db:
                            db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', file_path=file_path, status='failed', error='uploader missing')
                    else:
                        caption = f"DPP Backup: {subj_name} / {cname} / {lec_name}"
                        resp = tele_up.upload(file_path, caption=caption, as_video=True, progress_message=False, progress_meta={"server_id": os.getenv('HOSTNAME') or os.getenv('HOST'), "title": lec_name})
                        msg_chat = resp.get('chat_id') if isinstance(resp, dict) else None
                        msg_id = resp.get('message_id') if isinstance(resp, dict) else None
                        print('   -> uploaded', msg_chat, msg_id)
                        if db:
                            db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', file_path=file_path, file_size=os.path.getsize(file_path) if os.path.exists(file_path) else None, telegram_chat_id=str(msg_chat) if msg_chat else None, telegram_message_id=str(msg_id) if msg_id else None, metadata=json.dumps({'lecture_name': lec_name, 'chapter': cname, 'subject': subj_slug, 'videoDetails': _serialize(getattr(lecture,'videoDetails', None))}), status='done')
                finally:
                    if old is None:
                        os.environ.pop('TELEGRAM_CHAT_ID', None)
                    else:
                        os.environ['TELEGRAM_CHAT_ID'] = old
            else:
                # Only record metadata even if not uploading
                if db:
                    db.upsert_dpp_backup(batch_id, lec_id, kind='dpp', file_path=file_path, file_size=os.path.getsize(file_path) if file_path and os.path.exists(file_path) else None, metadata=json.dumps({'lecture_name': lec_name, 'chapter': cname, 'subject': subj_slug, 'videoDetails': _serialize(getattr(lecture,'videoDetails', None))}), status='pending')

        # --- Fetch DPP PDFs (attachments) for this chapter ---
        if args.dpp_pdfs:
            try:
                dpp_list = batch_api.process('dpp_pdf', use_model=True, batch_name=batch_slug, subject_name=subj_slug, chapter_name=cname)
            except Exception as e:
                print('   -> dpp_pdf fetch failed:', e)
                dpp_list = None
            if dpp_list:
                for dpp in dpp_list:
                    # Each dpp may contain homeworks with attachments
                    for hw in getattr(dpp, 'homeworks', []):
                        for att in getattr(hw, 'attachments', []):
                            url = getattr(att, 'link', None) or getattr(att, 'baseUrl', None)
                            name = getattr(att, 'name', None) or url.split('/')[-1]
                            if not url:
                                continue
                            dest = os.path.join(download_dir, name)
                            if not os.path.exists(dest):
                                print('   -> downloading DPP attachment', name)
                                try:
                                    ScraperModule().download_file(url, download_dir, name)
                                except Exception as e:
                                    print('      download failed:', e)
                                    if db:
                                        db.upsert_dpp_backup(batch_id, getattr(dpp, 'id', getattr(hw,'id', '')), kind='dpp_pdf', file_path=None, status='failed', error=str(e))
                                    continue
                            else:
                                print('   -> cached DPP attachment', dest)
                            # Upload if requested
                            if args.upload:
                                if not backup_chat:
                                    print('   -> no backup chat configured, skipping attachment upload')
                                    if db:
                                        db.upsert_dpp_backup(batch_id, getattr(dpp, 'id', getattr(hw,'id', '')), kind='dpp_pdf', file_path=dest, status='skipped', error='no backup chat')
                                    continue
                                old = os.environ.get('TELEGRAM_CHAT_ID')
                                os.environ['TELEGRAM_CHAT_ID'] = str(backup_chat)
                                try:
                                    if tele_up is None:
                                        print('   -> telegram uploader missing for DPP PDF')
                                        if db:
                                            db.upsert_dpp_backup(batch_id, getattr(dpp, 'id', getattr(hw,'id', '')), kind='dpp_pdf', file_path=dest, status='failed', error='uploader missing')
                                    else:
                                        caption = f"DPP PDF: {subj_name} / {cname} / {name}"
                                        resp = tele_up.upload(dest, caption=caption, as_video=False, progress_message=False, progress_meta={"server_id": os.getenv('HOSTNAME') or os.getenv('HOST'), "title": name})
                                        msg_chat = resp.get('chat_id') if isinstance(resp, dict) else None
                                        msg_id = resp.get('message_id') if isinstance(resp, dict) else None
                                        print('   -> uploaded attachment', msg_chat, msg_id)
                                        if db:
                                            db.upsert_dpp_backup(batch_id, getattr(dpp, 'id', getattr(hw,'id', '')), kind='dpp_pdf', file_path=dest, file_size=os.path.getsize(dest) if os.path.exists(dest) else None, telegram_chat_id=str(msg_chat) if msg_chat else None, telegram_message_id=str(msg_id) if msg_id else None, metadata=json.dumps({'attachment_name': name, 'chapter': cname, 'subject': subj_slug}), status='done')
                                finally:
                                    if old is None:
                                        os.environ.pop('TELEGRAM_CHAT_ID', None)
                                    else:
                                        os.environ['TELEGRAM_CHAT_ID'] = old

        # --- Discover & download all tests for this batch (optional) ---
        if args.download_all_tests:
            # Build test-list URL similar to report.pwdl.py but filtered for this batch
            try:
                from mainLogic.utils.Endpoint import Endpoint
                tests_url = (
                    f"https://api.penpencil.co/v3/test-service/tests?testType=All&testStatus=All&attemptStatus=All&batchId={batch_id}&isSubjective=false&isPurchased=true"
                )
                print('   -> fetching test list for batch')
                resp = Endpoint(url=tests_url, headers=batch_api.DEFAULT_HEADERS).fetch()
                data = resp[0].get('data', []) if resp and isinstance(resp, list) else []
                tests = data if data else []
            except Exception as e:
                print('   -> could not fetch test list:', e)
                tests = []

            for t in tests:
                tid = t.get('testStudentMappingId') or t.get('testMappingId') or t.get('id')
                tname = t.get('name') or tid
                print(f"   -> test: {tname} ({tid})")
                # Use existing test download logic: fetch preview and download images
                try:
                    test_data = batch_api.get_test(tid)
                except Exception as e:
                    print('      -> get_test failed:', e)
                    test_data = None
                if test_data and getattr(test_data, 'data', None):
                    test = test_data.data
                    questions = getattr(test, 'questions', [])
                    subject_dir = os.path.join(download_dir, f"tests/{subj_slug}")
                    os.makedirs(subject_dir, exist_ok=True)
                    for i, q in enumerate(questions or []):
                        link = getattr(getattr(q, 'question', None), 'imageIds', None)
                        if link and getattr(link, 'link', None):
                            fname = getattr(link, 'name', f"{tid}_q{i:03d}.png")
                            dest = os.path.join(subject_dir, fname)
                            if not os.path.exists(dest):
                                print('      -> downloading question image', fname)
                                try:
                                    ScraperModule().download_file(link.link, subject_dir, fname)
                                except Exception as e:
                                    print('         download failed:', e)
                                    if db:
                                        db.upsert_dpp_backup(batch_id, f"test:{tid}:q{i}", kind='test_question', file_path=None, status='failed', error=str(e))
                                    continue
                            if args.upload:
                                # upload per-question to backup chat
                                if not backup_chat:
                                    print('      -> no backup chat configured, skipping question upload')
                                    if db:
                                        db.upsert_dpp_backup(batch_id, f"test:{tid}:q{i}", kind='test_question', file_path=dest, status='skipped', error='no backup chat')
                                    continue
                                old = os.environ.get('TELEGRAM_CHAT_ID')
                                os.environ['TELEGRAM_CHAT_ID'] = str(backup_chat)
                                try:
                                    if tele_up is None:
                                        print('      -> telegram uploader missing for question')
                                        if db:
                                            db.upsert_dpp_backup(batch_id, f"test:{tid}:q{i}", kind='test_question', file_path=dest, status='failed', error='uploader missing')
                                    else:
                                        caption = f"{tname} ({tid}) Q{i+1}"
                                        resp = tele_up.upload(dest, caption=caption, as_video=False, progress_message=False, progress_meta={"server_id": os.getenv('HOSTNAME') or os.getenv('HOST'), "title": caption})
                                        msg_chat = resp.get('chat_id') if isinstance(resp, dict) else None
                                        msg_id = resp.get('message_id') if isinstance(resp, dict) else None
                                        print('      -> uploaded question', msg_chat, msg_id)
                                        if db:
                                            db.upsert_dpp_backup(batch_id, f"test:{tid}:q{i}", kind='test_question', file_path=dest, file_size=os.path.getsize(dest) if os.path.exists(dest) else None, telegram_chat_id=str(msg_chat) if msg_chat else None, telegram_message_id=str(msg_id) if msg_id else None, metadata=json.dumps({'question_index': i, 'test_id': tid, 'test_name': tname}), status='done')
                                finally:
                                    if old is None:
                                        os.environ.pop('TELEGRAM_CHAT_ID', None)
                                    else:
                                        os.environ['TELEGRAM_CHAT_ID'] = old

print('Done')
