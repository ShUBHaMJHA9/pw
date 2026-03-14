#!/usr/bin/env python3
"""
DPP (Daily Practice Problem) Downloader & Uploader

Fetches DPP data from PW Batch API:
  - DPP notes metadata
  - Problem attachments (question PDFs, solution PDFs)
  - Solution videos
  
Downloads files locally and uploads to Internet Archive,
storing metadata in database.

Usage:
  python dpp-dl.pwdl.py --batch BATCH_SLUG [--user USER] [--download] [--upload] [--db-log]
  
Example:
  python dpp-dl.pwdl.py --batch jee-main --user "ADITYA RAJ" --download --upload --db-log
"""

import argparse
import json
import os
import sys
import tempfile
import time
import mimetypes
from datetime import datetime, timezone
from pprint import pprint

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

import requests
from beta.batch_scraper_2.module import ScraperModule
from beta.batch_scraper_2.Endpoints import Endpoints
from mainLogic.utils.glv_var import PREFS_FILE

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from mainLogic.downloader import main as downloader
from mainLogic.utils.gen_utils import generate_safe_folder_name

# --- Setup ---
parser = argparse.ArgumentParser(description="DPP downloader and uploader")
parser.add_argument('--batch', required=True, help='Batch slug')
parser.add_argument('--user', type=str, help='User profile to use')
parser.add_argument('--download', action='store_true', help='Download DPP files')
parser.add_argument('--upload', action='store_true', help='Upload to Internet Archive')
parser.add_argument('--db-log', action='store_true', help='Log to database')
args = parser.parse_args()

prefs = ScraperModule.prefs
debugger = ScraperModule.debugger
batch_api = ScraperModule.batch_api
current_user_info = None

console = Console() if Console else None

# --- User Selection ---
def _select_user(api, user_spec=None):
    """Select user by name or index"""
    global current_user_info
    users = prefs.get('users', []) if isinstance(prefs, dict) else []
    
    if not users:
        return api
    
    if user_spec:
        try:
            idx = int(user_spec) - 1
            if 0 <= idx < len(users):
                chosen = users[idx]
            else:
                debugger.error("User index out of range")
                chosen = users[0]
        except:
            chosen = next((u for u in users if str(u.get('name')) == user_spec), users[0])
    else:
        chosen = users[0]
    
    current_user_info = chosen
    token = chosen.get('token') or chosen.get('access_token')
    random_id = chosen.get('random_id') or chosen.get('randomId')
    
    if token:
        try:
            return Endpoints(verbose=False).set_token(token, random_id=random_id)
        except Exception as e:
            debugger.error(f"Failed to set token: {e}")
            return api
    return api

if args.user:
    batch_api = _select_user(batch_api, args.user)

# --- DB Logger Setup ---
db = None
if args.db_log:
    try:
        from mainLogic.utils import mysql_logger as db_module
        db_module.init(None)
        db_module.ensure_schema()
        db = db_module
    except Exception as e:
        debugger.error(f"DB logger setup failed: {e}")
        db = None

# --- Helper Functions ---

def _get_safe_filename(name):
    """Convert name to safe filename"""
    if not name:
        return "file"
    safe = "".join(c if c.isalnum() or c in '._-' else '_' for c in name)
    return safe[:200]

def _download_file(url, dest_path, timeout=30):
    """Download file with error handling"""
    try:
        if not url:
            return False
        
        resp = requests.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()
        
        os.makedirs(os.path.dirname(dest_path) or '.', exist_ok=True)
        
        with open(dest_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return True
    except Exception as e:
        debugger.error(f"Download failed for {url}: {e}")
        return False

def _get_file_size(path):
    """Get file size safely"""
    try:
        return os.path.getsize(path) if os.path.exists(path) else None
    except:
        return None

def _get_mime_type(filename):
    """Get MIME type for file"""
    if not filename:
        return None
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"

# --- DPP Fetching ---

def _fetch_dpp_notes(api, batch_slug, subject_name, chapter_name):
    """Fetch DPP notes for a chapter"""
    try:
        result = api.process(
            type="dpp_pdf",
            batch_name=batch_slug,
            subject_name=subject_name,
            chapter_name=chapter_name
        )
        debugger.info(f"Fetched DPP notes for {subject_name}/{chapter_name}: {len(result or [])} items")
        return result or []
    except Exception as e:
        debugger.error(f"Failed to fetch DPP notes: {e}")
        return []

def _process_dpp_notes(batch_id, batch_slug, subject_name, chapter_name, dpp_list):
    """Process DPP data: download files and store in DB"""
    if not dpp_list:
        debugger.info("No DPP notes found")
        return
    
    debugger.info(f"Processing {len(dpp_list)} DPP notes")
    
    for dpp_item in dpp_list:
        try:
            dpp_id = dpp_item.get('_id') or dpp_item.get('id')
            dpp_date = dpp_item.get('date')
            start_time = dpp_item.get('startTime')
            
            if not dpp_id:
                continue
            
            debugger.info(f"\n[DPP {dpp_id}] Date: {dpp_date}")
            
            # Store DPP notes metadata
            if db:
                db.upsert_dpp_notes(
                    batch_id=batch_id,
                    dpp_id=dpp_id,
                    batch_slug=batch_slug,
                    subject_name=subject_name,
                    chapter_name=chapter_name,
                    dpp_date=dpp_date,
                    start_time=start_time,
                    is_batch_doubt_enabled=dpp_item.get('isBatchDoubtEnabled', False),
                    is_dpp_notes=dpp_item.get('isDPPNotes', True),
                    is_free=dpp_item.get('isFree', False),
                    is_simulated_lecture=dpp_item.get('isSimulatedLecture', False),
                    status='fetched'
                )
            
            # Process homeworks (problems)
            homeworks = dpp_item.get('homeworkIds', [])
            debugger.info(f"  Homeworks: {len(homeworks)}")
            
            for problem_idx, homework_item in enumerate(homeworks, 1):
                problem_id = homework_item.get('_id') or homework_item.get('id')
                if not problem_id:
                    continue
                
                topic = homework_item.get('topic')
                note = homework_item.get('note')
                solution_video_id = homework_item.get('solutionVideoId')
                solution_video_url = homework_item.get('solutionVideoUrl')
                solution_video_s3_url = homework_item.get('solutionVideoS3Url')
                
                debugger.info(f"    Problem {problem_idx}: {problem_id} - {topic}")
                
                # Store problem metadata
                if db:
                    db.upsert_dpp_problem(
                        batch_id=batch_id,
                        dpp_id=dpp_id,
                        problem_id=problem_id,
                        problem_number=problem_idx,
                        topic=topic,
                        note=note,
                        has_solution_video=bool(solution_video_id),
                        solution_video_id=solution_video_id,
                        solution_video_url=solution_video_url,
                        solution_video_s3_url=solution_video_s3_url,
                        status='fetched'
                    )
                
                # Process attachments (PDFs)
                attachments = homework_item.get('attachmentIds', [])
                debugger.info(f"      Attachments: {len(attachments)}")
                
                for att_item in attachments:
                    att_id = att_item.get('_id') or att_item.get('id')
                    att_name = att_item.get('name')
                    att_base_url = att_item.get('baseUrl')
                    att_key = att_item.get('key')
                    
                    if att_id:
                        debugger.info(f"        Attachment: {att_name}")
                        
                        # Store attachment metadata
                        if db:
                            db.upsert_dpp_attachment(
                                batch_id=batch_id,
                                dpp_id=dpp_id,
                                problem_id=problem_id,
                                attachment_id=att_id,
                                attachment_name=att_name,
                                base_url=att_base_url,
                                source_key=att_key,
                                source_url=f"{att_base_url}{att_key}" if att_base_url and att_key else None,
                                status='fetched'
                            )
                
                # Process solution video
                if solution_video_id:
                    debugger.info(f"      Solution Video: {solution_video_id}")
                    
                    if db:
                        db.upsert_dpp_solution_video(
                            batch_id=batch_id,
                            dpp_id=dpp_id,
                            problem_id=problem_id,
                            video_id=solution_video_id,
                            video_type=homework_item.get('solutionVideoType'),
                            source_url=solution_video_url,
                            s3_url=solution_video_s3_url,
                            status='fetched'
                        )
        
        except Exception as e:
            debugger.error(f"Error processing DPP {dpp_item.get('_id')}: {e}")

def _download_dpp_files(batch_id, batch_slug, download_dir):
    """Download all DPP files (attachments and videos)"""
    if not db:
        debugger.error("DB logging required for download tracking")
        return
    
    # Get pending attachments
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT batch_id, dpp_id, problem_id, attachment_id, attachment_name,
                       source_url, status
                FROM dpp_attachments
                WHERE batch_id=%s AND status IN ('fetched', 'failed')
                ORDER BY created_at ASC
                LIMIT 100
                """,
                (batch_id,)
            )
            attachments = cur.fetchall() or []
    finally:
        conn.close()
    
    debugger.info(f"Downloading {len(attachments)} DPP attachments")
    
    for att in attachments:
        try:
            dpp_id = att.get('dpp_id')
            problem_id = att.get('problem_id')
            att_id = att.get('attachment_id')
            att_name = att.get('attachment_name') or 'file.pdf'
            source_url = att.get('source_url')
            
            if not source_url:
                debugger.warn(f"No URL for attachment {att_id}")
                continue
            
            # Build local path
            safe_dir = os.path.join(download_dir, batch_slug, dpp_id, problem_id)
            safe_filename = _get_safe_filename(att_name)
            local_path = os.path.join(safe_dir, safe_filename)
            
            # Download
            debugger.info(f"Downloading: {safe_filename}")
            if _download_file(source_url, local_path):
                file_size = _get_file_size(local_path)
                file_mime = _get_mime_type(local_path)
                
                db.upsert_dpp_attachment(
                    batch_id=batch_id,
                    dpp_id=dpp_id,
                    problem_id=problem_id,
                    attachment_id=att_id,
                    attachment_name=att_name,
                    source_url=source_url,
                    file_path=local_path,
                    file_size=file_size,
                    file_mime=file_mime,
                    status='downloaded'
                )
                debugger.info(f"  → {local_path} ({file_size} bytes)")
            else:
                db.upsert_dpp_attachment(
                    batch_id=batch_id,
                    dpp_id=dpp_id,
                    problem_id=problem_id,
                    attachment_id=att_id,
                    status='failed',
                    error_text='Download failed'
                )
        except Exception as e:
            debugger.error(f"Error downloading attachment: {e}")

def _main():
    """Main flow"""
    # Get batch info
    batches = batch_api.get_purchased_batches(all_pages=True)
    batch_id = None
    batch_info = None
    
    for b in batches or []:
        if b.get('slug') == args.batch or b.get('_id') == args.batch:
            batch_id = b.get('_id')
            batch_info = b
            break
    
    if not batch_id:
        debugger.error(f"Batch '{args.batch}' not found")
        sys.exit(1)
    
    debugger.info(f"Found batch: {batch_info.get('name')} ({batch_id})")
    
    # Get subjects
    subjects = batch_api.get_batch_details(batch_name=args.batch) or []
    debugger.info(f"Subjects: {len(subjects)}")
    
    downloads_dir = os.path.join(os.getcwd(), 'dpp_downloads')
    os.makedirs(downloads_dir, exist_ok=True)
    
    # Process each subject and chapter
    total_dpp = 0
    for subject in subjects:
        subject_name = subject.name or subject.get('name')
        chapters = subject.chapters or subject.get('chapters', [])
        
        debugger.info(f"\n[Subject] {subject_name} ({len(chapters)} chapters)")
        
        for chapter in chapters:
            chapter_name = chapter.name or chapter.get('name')
            
            debugger.info(f"  [Chapter] {chapter_name}")
            
            # Fetch DPP notes for this chapter
            dpp_list = _fetch_dpp_notes(batch_api, args.batch, subject_name, chapter_name)
            
            if dpp_list:
                total_dpp += len(dpp_list)
                _process_dpp_notes(batch_id, args.batch, subject_name, chapter_name, dpp_list)
    
    debugger.info(f"\n✓ Fetched and stored metadata for {total_dpp} DPP notes")
    
    # Download files if requested
    if args.download:
        _download_dpp_files(batch_id, args.batch, downloads_dir)
    
    debugger.info("\nDPP download complete!")

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        print("\n✗ Interrupted by user")
        sys.exit(130)
    except Exception as e:
        debugger.error(f"Fatal error: {e}")
        sys.exit(1)
