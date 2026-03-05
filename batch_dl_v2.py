#!/usr/bin/env python3
"""
Batch Content Downloader with Internet Archive Integration
Downloads notes, DPP, and tests from a batch with proper organization.
Uploads to IA and stores metadata in database.

Usage: python batch_dl_v2.py [--user USER] [--batch BATCH] [--subject SUBJECT] [--type TYPE] [--output DIR] [--ia] [--db]
"""

from types import SimpleNamespace
import json
import os
import sys
import time
import tarfile
import requests
import hashlib
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

sys.path.insert(0, os.path.dirname(__file__))

from beta.batch_scraper_2.module import ScraperModule
from beta.batch_scraper_2.Endpoints import Endpoints
from mainLogic.utils.glv_var import PREFS_FILE, debugger
from mainLogic.utils.gen_utils import generate_safe_folder_name
from mainLogic.utils.internet_archive_uploader import upload_file, identifier_dash

try:
    from batch_download_db import BatchDownloadDB
    HAS_DB = True
except ImportError:
    HAS_DB = False
    debugger.warning("Database module not available")

prefs = ScraperModule.prefs

# ============================================================================
# CONFIGURATION
# ============================================================================
args = SimpleNamespace(
    user=None,
    batch=None,            # if None, first purchased batch will be used
    subject=None,          # None => all subjects
    type='all',
    output='./downloads',
    ia=True,               # always upload to Internet Archive
    db=True,               # always store metadata in DB when available
    skip_existing=False
)

# mapping for content directories inside batch output folder
CONTENT_DIR_MAPPING = {
    'notes': 'notes',
    'dpp': 'dpp',
    'tests': 'tests'
}

def init_api():
    """Initialize and authenticate API"""
    users = prefs.get('users', []) if isinstance(prefs, dict) else []
    
    if not users:
        debugger.warning("No user profiles found")
        return ScraperModule.batch_api
    
    # Get user
    user = users[0]  # Default to first user for now
    token = user.get('access_token') or user.get('token')
    random_id = user.get('random_id')
    
    if not token:
        debugger.error("No token found")
        return ScraperModule.batch_api
    
    api = Endpoints(verbose=False)
    if random_id:
        return api.set_token(token, random_id=random_id)
    return api.set_token(token)


batch_api = init_api()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_file_extension(url, default='pdf'):
    """Extract file extension from URL or default"""
    try:
        # Get extension from URL path
        path = url.split('?')[0]  # Remove query params
        ext = path.split('.')[-1].lower()
        if ext and len(ext) <= 5:  # Valid extension
            return ext
    except:
        pass
    return default


def sanitize_filename(filename, extension=None):
    """
    Sanitize filename and ensure proper extension.
    If filename already has extension, don't duplicate it.
    """
    if not filename:
        filename = "document"
    
    # Remove any trailing extension if present
    base = filename.rsplit('.', 1)[0] if '.' in filename else filename
    
    # Use provided extension or extract from filename
    if extension:
        ext = extension.lstrip('.')
    else:
        # Try to get extension from original filename
        if '.' in filename:
            ext = filename.rsplit('.', 1)[1].strip().lstrip('.')
        else:
            ext = 'pdf'
    
    # Ensure extension is not empty
    if not ext:
        ext = 'pdf'
    
    return f"{base}.{ext}"


def download_file(url, output_path, timeout=30):
    """Download file and return True if successful"""
    try:
        response = requests.get(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        size_mb = len(response.content) / (1024 * 1024)
        debugger.success(f"✓ Downloaded: {os.path.basename(output_path)} ({size_mb:.1f}MB)")
        return True
    
    except Exception as e:
        debugger.warning(f"✗ Failed to download: {e}")
        return False


# ============================================================================
# NOTE DOWNLOAD
# ============================================================================

def download_notes_for_chapter(batch_slug, batch_name, subject_name, chapter_name, output_dir, batch_id=None, subject_id=None, chapter_id=None, db=None):
    """Download notes for a specific chapter and store in database"""
    try:
        notes = batch_api.get_batch_notes(
            batch_name=batch_slug,
            subject_name=subject_name,
            chapter_name=chapter_name
        )
        
        if not notes:
            return 0
        
        notes_dir = os.path.join(output_dir, CONTENT_DIR_MAPPING['notes'])
        os.makedirs(notes_dir, exist_ok=True)
        
        count = 0
        for note in notes:
            try:
                # Store note in database if available
                if db and batch_id and subject_id and chapter_id:
                    db.add_note(
                        note_id=note.id,
                        batch_id=batch_id,
                        subject_id=subject_id,
                        chapter_id=chapter_id,
                        note_date=str(note.date) if hasattr(note, 'date') and note.date else None
                    )
                
                homeworks = note.homeworks if hasattr(note, 'homeworks') else []
                
                for hw in homeworks:
                    attachments = hw.attachments if hasattr(hw, 'attachments') else []
                    
                    for attach in attachments:
                        if not hasattr(attach, 'link'):
                            continue
                        
                        url = attach.link
                        name = attach.name if hasattr(attach, 'name') else f"note_{note.id}"
                        ext = get_file_extension(url, 'pdf')
                        filename = sanitize_filename(name, ext)
                        
                        fpath = os.path.join(notes_dir, filename)
                        
                        if args.skip_existing and os.path.exists(fpath):
                            continue
                        
                        if download_file(url, fpath):
                            count += 1
                            
                            # Store attachment in database
                            if db:
                                att_id = hashlib.md5(f"{note.id}_{attach.id if hasattr(attach, 'id') else filename}".encode()).hexdigest()[:16]
                                db.add_note_attachment(
                                    attachment_id=att_id,
                                    note_id=note.id,
                                    batch_id=batch_id,
                                    file_name=filename,
                                    file_url=url,
                                    file_path=fpath,
                                    file_size=os.path.getsize(fpath),
                                    file_ext=ext
                                )

                                # Immediately upload this file to Internet Archive and record it
                                if args.ia:
                                    try:
                                        ia_id_base = identifier_dash(f"pw-{batch_slug}-{note.id}-{attach.id if hasattr(attach, 'id') else filename}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
                                        ia_identifier = ia_id_base[:80]
                                        result_identifier = upload_file(file_path=fpath, identifier=ia_identifier, title=f"{batch_name} - {filename}")
                                        if result_identifier:
                                            ia_identifier = result_identifier
                                            ia_url = f"https://archive.org/details/{ia_identifier}"
                                            db.add_ia_upload(
                                                ia_id=hashlib.md5(ia_identifier.encode()).hexdigest()[:16],
                                                ia_identifier=ia_identifier,
                                                ia_url=ia_url,
                                                batch_id=batch_id,
                                                subject_id=subject_id,
                                                chapter_id=chapter_id,
                                                note_id=note.id,
                                                attachment_id=att_id,
                                                ia_title=filename,
                                                file_count=1,
                                                total_size=os.path.getsize(fpath)
                                            )
                                            debugger.success(f"✓ Uploaded to IA: {ia_url}")
                                    except Exception as e:
                                        debugger.warning(f"IA upload failed for {filename}: {e}")
            
            except Exception as e:
                debugger.warning(f"Error processing note: {e}")
        
        return count
    
    except Exception as e:
        debugger.error(f"Error downloading notes: {e}")
        return 0


# ============================================================================
# DPP DOWNLOAD
# ============================================================================

def download_dpp_for_chapter(batch_slug, batch_name, subject_name, chapter_name, output_dir, batch_id=None, subject_id=None, chapter_id=None, db=None):
    """Download DPP notes for a specific chapter and store in database"""
    try:
        dpp_list = batch_api.get_batch_dpp_notes(
            batch_name=batch_slug,
            subject_name=subject_name,
            chapter_name=chapter_name
        )
        
        if not dpp_list:
            return 0
        
        dpp_dir = os.path.join(output_dir, CONTENT_DIR_MAPPING['dpp'])
        os.makedirs(dpp_dir, exist_ok=True)
        
        count = 0
        for dpp in dpp_list:
            try:
                # Store DPP in database if available
                if db and batch_id and subject_id and chapter_id:
                    db.add_dpp(
                        dpp_id=dpp.id,
                        batch_id=batch_id,
                        subject_id=subject_id,
                        chapter_id=chapter_id,
                        dpp_date=str(dpp.date) if hasattr(dpp, 'date') and dpp.date else None
                    )
                
                homeworks = dpp.homeworks if hasattr(dpp, 'homeworks') else []
                
                for hw in homeworks:
                    attachments = hw.attachments if hasattr(hw, 'attachments') else []
                    
                    for attach in attachments:
                        if not hasattr(attach, 'link'):
                            continue
                        
                        url = attach.link
                        name = attach.name if hasattr(attach, 'name') else f"dpp_{dpp.id}"
                        ext = get_file_extension(url, 'pdf')
                        filename = sanitize_filename(name, ext)
                        
                        fpath = os.path.join(dpp_dir, filename)
                        
                        if args.skip_existing and os.path.exists(fpath):
                            continue
                        
                        if download_file(url, fpath):
                            count += 1
                            
                            # Store attachment in database
                            if db:
                                att_id = hashlib.md5(f"{dpp.id}_{attach.id if hasattr(attach, 'id') else filename}".encode()).hexdigest()[:16]

                                # compute file hash to assist deduplication
                                try:
                                    with open(fpath, 'rb') as fh:
                                        file_bytes = fh.read()
                                        file_hash = hashlib.md5(file_bytes).hexdigest()
                                except Exception:
                                    file_hash = None

                                db.add_dpp_attachment(
                                    attachment_id=att_id,
                                    dpp_id=dpp.id,
                                    batch_id=batch_id,
                                    file_name=filename,
                                    file_url=url,
                                    file_path=fpath,
                                    file_size=os.path.getsize(fpath),
                                    file_ext=ext
                                )

                                # Immediately upload this DPP file to Internet Archive and record it
                                if args.ia:
                                    try:
                                        # Check for existing upload by file hash or attachment id
                                        already = None
                                        try:
                                            already = db.get_ia_by_file_hash(file_hash) if file_hash and db else None
                                        except Exception:
                                            already = None

                                        if not already and db:
                                            try:
                                                already = db.get_ia_by_attachment(att_id)
                                            except Exception:
                                                already = None

                                        if already and already.get('ia_identifier'):
                                            ia_identifier = already.get('ia_identifier')
                                            ia_url = already.get('ia_url') or f"https://archive.org/details/{ia_identifier}"
                                            debugger.info(f"✓ Skipping upload, already on IA: {ia_url}")
                                        else:
                                            ia_id_base = identifier_dash(f"pw-{batch_slug}-dpp-{dpp.id}-{attach.id if hasattr(attach, 'id') else filename}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
                                            ia_identifier = ia_id_base[:80]
                                            result_identifier = upload_file(file_path=fpath, identifier=ia_identifier, title=f"{batch_name} - {filename}")
                                            if result_identifier:
                                                ia_identifier = result_identifier
                                                ia_url = f"https://archive.org/details/{ia_identifier}"
                                                db.add_ia_upload(
                                                    ia_id=hashlib.md5(ia_identifier.encode()).hexdigest()[:16],
                                                    ia_identifier=ia_identifier,
                                                    ia_url=ia_url,
                                                    batch_id=batch_id,
                                                    subject_id=subject_id,
                                                    chapter_id=chapter_id,
                                                    dpp_id=dpp.id,
                                                    attachment_id=att_id,
                                                    ia_title=filename,
                                                    file_count=1,
                                                    total_size=os.path.getsize(fpath),
                                                    file_hash=file_hash
                                                )
                                                debugger.success(f"✓ Uploaded to IA: {ia_url}")
                                    except Exception as e:
                                        debugger.warning(f"IA upload failed for {filename}: {e}")
            
            except Exception as e:
                debugger.warning(f"Error processing DPP: {e}")
        
        return count
    
    except Exception as e:
        debugger.error(f"Error downloading DPP: {e}")
        return 0


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution"""
    print("\n" + "=" * 70)
    print("  BATCH CONTENT DOWNLOADER v2")
    print("=" * 70 + "\n")
    
    # Initialize database
    db = None
    if HAS_DB:
        try:
            db = BatchDownloadDB()
            if db.connection:
                print("✓ Database connected\n")
        except Exception as db_err:
            debugger.warning(f"Database connection failed: {db_err}")
            db = None
    
    # Get batches
    print("📥 Loading purchased batches...")
    batches = batch_api.get_purchased_batches(all_pages=True)
    
    if not batches:
        debugger.error("No batches found")
        sys.exit(1)
    
    # Select batch
    if args.batch:
        batch_slug = args.batch
        batch_name = args.batch
        debugger.info(f"Using batch: {batch_slug}")
    else:
        print(f"\nFound {len(batches)} batches:")
        for idx, b in enumerate(batches[:10], 1):
            print(f"  {idx}. {b.get('name', 'Unknown')[:50]}")
        
        choice = input("\nSelect batch [1]: ").strip() or "1"
        try:
            batch = batches[int(choice) - 1]
            batch_slug = batch.get('slug')
            batch_name = batch.get('name', 'Unknown')
        except (ValueError, IndexError):
            debugger.error("Invalid choice")
            sys.exit(1)
    
    print(f"\n✓ Selected: {batch_name}\n")
    
    # Create batch ID for database
    batch_id = hashlib.md5(batch_slug.encode()).hexdigest()[:16]
    
    # Store batch in database
    if db:
        db.add_batch(batch_id, batch_slug, batch_name)
    
    # Get subjects
    print("📚 Loading subjects...")
    subjects = batch_api.get_batch_details(batch_name=batch_slug)
    
    if not subjects:
        debugger.error("No subjects found")
        sys.exit(1)
    
    # Extract subject names and store in database
    subject_names = []
    subject_map = {}  # slug -> (object, db_id)
    
    for s in subjects:
        name = None
        if hasattr(s, 'name') and s.name:
            name = s.name
        elif hasattr(s, 'slug') and s.slug:
            name = s.slug
        elif hasattr(s, 'qbgSubjectId'):
            name = s.qbgSubjectId
        
        if name:
            subject_names.append(str(name))
            
            # Store subject in database
            subject_db_id = hashlib.md5(f"{batch_id}{name}".encode()).hexdigest()[:16]
            subject_map[name] = (s, subject_db_id)
            
            if db:
                db.add_subject(
                    subject_id=subject_db_id,
                    batch_id=batch_id,
                    slug=name,
                    name=name
                )
    
    if not subject_names:
        debugger.error("No valid subjects found")
        sys.exit(1)
    
    # Select subjects
    if args.subject:
        selected_subjects = args.subject
    else:
        print(f"\nFound {len(subject_names)} subjects:")
        for idx, name in enumerate(subject_names[:20], 1):
            print(f"  {idx:2}. {name}")
        
        if len(subject_names) > 20:
            print(f"  ... and {len(subject_names) - 20} more")
        
        choice = input("\nSelect subjects (comma-separated) [all]: ").strip().lower() or "all"
        
        if choice == 'all':
            selected_subjects = subject_names
        else:
            try:
                indices = [int(x.strip()) - 1 for x in choice.split(',')]
                selected_subjects = [subject_names[i] for i in indices if 0 <= i < len(subject_names)]
            except ValueError:
                selected_subjects = subject_names
    
    print(f"\n✓ Selected {len(selected_subjects)} subjects")
    
    # Create output directory
    safe_batch_name = generate_safe_folder_name(batch_slug)
    batch_output_dir = os.path.join(args.output, safe_batch_name)
    os.makedirs(batch_output_dir, exist_ok=True)
    
    print(f"\n📂 Output: {os.path.abspath(batch_output_dir)}")
    print(f"📥 Types: {args.type}")
    print(f"🎯 Subjects: {', '.join(selected_subjects[:3])}{'...' if len(selected_subjects) > 3 else ''}\n")
    
    # Download content
    print("=" * 70)
    print("  DOWNLOADING CONTENT")
    print("=" * 70 + "\n")
    
    total_downloaded = 0
    start_time = time.time()
    
    for subject_idx, subject_name in enumerate(selected_subjects, 1):
        print(f"\n📌 Subject [{subject_idx}/{len(selected_subjects)}]: {subject_name}")
        
        subject_dir = os.path.join(batch_output_dir, generate_safe_folder_name(subject_name))
        os.makedirs(subject_dir, exist_ok=True)
        
        # Get database IDs
        subject_obj, subject_db_id = subject_map.get(subject_name, (None, None))
        
        # Get chapters
        try:
            chapters = batch_api.get_batch_subjects(batch_name=batch_slug, subject_name=subject_name)
        except Exception as e:
            debugger.warning(f"Failed to get chapters: {e}")
            continue
        
        if not chapters:
            debugger.info("No chapters found")
            continue
        
        for chapter in chapters:
            chapter_name = chapter.name if hasattr(chapter, 'name') else chapter.get('name', 'Unknown')
            chapter_dir = os.path.join(subject_dir, generate_safe_folder_name(chapter_name))
            os.makedirs(chapter_dir, exist_ok=True)
            
            # Store chapter in database
            chapter_db_id = hashlib.md5(f"{subject_db_id}{chapter_name}".encode()).hexdigest()[:16]
            if db and subject_db_id:
                db.add_chapter(
                    chapter_id=chapter_db_id,
                    subject_id=subject_db_id,
                    batch_id=batch_id,
                    slug=chapter_name,
                    name=chapter_name
                )
            
            print(f"  📖 Chapter: {chapter_name}")
            
            # Download notes
            if args.type in ('notes', 'all'):
                count = download_notes_for_chapter(
                    batch_slug, batch_name, subject_name, chapter_name, chapter_dir,
                    batch_id=batch_id, subject_id=subject_db_id, chapter_id=chapter_db_id, db=db
                )
                if count > 0:
                    print(f"    ✓ Notes: {count} files")
                    total_downloaded += count
            
            # Download DPP
            if args.type in ('dpp', 'all'):
                count = download_dpp_for_chapter(
                    batch_slug, batch_name, subject_name, chapter_name, chapter_dir,
                    batch_id=batch_id, subject_id=subject_db_id, chapter_id=chapter_db_id, db=db
                )
                if count > 0:
                    print(f"    ✓ DPP: {count} files")
                    total_downloaded += count
    
    elapsed = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 70)
    print("  DOWNLOAD COMPLETE")
    print("=" * 70)
    print(f"✓ Downloaded {total_downloaded} files in {elapsed:.1f}s")
    print(f"📂 Location: {os.path.abspath(batch_output_dir)}\n")
    
    # Save metadata (always, even without IA upload)
    metadata = {
        'batch': {
            'name': batch_name,
            'slug': batch_slug
        },
        'download': {
            'date': datetime.now().isoformat(),
            'total_files': total_downloaded,
            'duration_seconds': elapsed,
            'file_location': os.path.abspath(batch_output_dir),
            'subjects_downloaded': selected_subjects
        },
        'content_types': {
            'notes': args.type in ('notes', 'all'),
            'dpp': args.type in ('dpp', 'all'),
            'lectures': args.type in ('lectures', 'all'),
            'tests': args.type in ('tests', 'all')
        }
    }
    
    # Add IA upload info if applicable
    ia_upload_info = None
    
    # Internet Archive upload
    if args.ia:
        print("📤 Uploading to Internet Archive...\n")
        ia_identifier = None
        ia_url = None
        
        try:
            # Create tar archive
            ia_identifier_base = identifier_dash(f"pw-batch-{batch_slug}-{datetime.now().strftime('%Y%m%d')}")
            ia_identifier = ia_identifier_base[:80]
            tar_path = f"{batch_output_dir}.tar.gz"
            
            print(f"📦 Creating archive: {tar_path}...")
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(batch_output_dir, arcname=os.path.basename(batch_output_dir))
            
            tar_size_gb = os.path.getsize(tar_path) / (1024**3)
            print(f"✓ Archive created: {tar_size_gb:.2f} GB\n")
            
            # Upload to Internet Archive
            print("⏳ Uploading to Internet Archive...")
            result_identifier = upload_file(
                file_path=tar_path,
                identifier=ia_identifier,
                title=f"PW Batch: {batch_name}"
            )
            
            if result_identifier:
                ia_identifier = result_identifier
                ia_url = f"https://archive.org/details/{ia_identifier}"
                print(f"✓ Uploaded: {ia_url}\n")
                
                # Store in database (always, if DB available)
                if db:
                    try:
                        # Add IA upload record for batch
                        ia_record_id = hashlib.md5(f"{ia_identifier}".encode()).hexdigest()[:16]
                        db.add_ia_upload(
                            ia_id=ia_record_id,
                            ia_identifier=ia_identifier,
                            ia_url=ia_url,
                            batch_id=batch_id,
                            ia_title=f"PW Batch: {batch_name}",
                            file_count=total_downloaded,
                            total_size=int(os.path.getsize(tar_path))
                        )
                        print(f"✓ IA metadata stored in database\n")
                    except Exception as db_err:
                        debugger.warning(f"Failed to store IA metadata: {db_err}")
                
                # Update metadata with IA info
                metadata['internet_archive'] = {
                    'identifier': ia_identifier,
                    'url': ia_url,
                    'archive_size_gb': round(tar_size_gb, 2),
                    'uploaded_at': datetime.now().isoformat(),
                    'status': 'success'
                }
            
            # Cleanup tar
            if os.path.exists(tar_path):
                os.remove(tar_path)
                print("🧹 Cleaned up temporary archive\n")
        
        except Exception as e:
            debugger.warning(f"IA upload failed: {e}\n")
            metadata['internet_archive'] = {'status': 'failed', 'error': str(e)}
            metadata['internet_archive'] = {
                'status': 'failed',
                'error': str(e),
                'identifier': ia_identifier
            }
    
    # Save metadata to file
    metadata_file = os.path.join(batch_output_dir, 'metadata.json')
    try:
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"✓ Metadata saved: {os.path.relpath(metadata_file)}\n")
    except Exception as e:
        debugger.warning(f"Failed to save metadata: {e}\n")


if __name__ == "__main__":
    import requests
    main()
