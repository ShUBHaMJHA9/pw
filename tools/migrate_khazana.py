#!/usr/bin/env python3
"""
Khazana Database Migration Tool
Migrates from old flat schema to new normalized schema
"""

import sys
import os
import re

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mainLogic.utils import mysql_logger


def clean_subject_name(subject_name):
    """Extract clean subject name by removing 'by Teacher' suffix"""
    if not subject_name:
        return None
    # Remove "by XYZ" pattern
    clean = re.sub(r'\s+by\s+.*$', '', subject_name, flags=re.IGNORECASE).strip()
    return clean if clean else None


def make_slug(text):
    """Convert text to URL-friendly slug"""
    if not text:
        return None
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')


def migrate_khazana_schema():
    """
    Migrate old khazana_lecture_uploads to new normalized schema
    """
    print("=" * 80)
    print("Khazana Database Migration")
    print("=" * 80)
    
    conn = mysql_logger._connect()
    try:
        with conn.cursor() as cur:
            # Check if old table exists
            cur.execute("SHOW TABLES LIKE 'khazana_lecture_uploads'")
            old_table_exists = bool(cur.fetchone())
            
            cur.execute("SHOW TABLES LIKE 'khazana_lecture_uploads_old'")
            backup_table_exists = bool(cur.fetchone())
            
            if not old_table_exists and not backup_table_exists:
                print("❌ Neither 'khazana_lecture_uploads' nor 'khazana_lecture_uploads_old' found.")
                print("   No data to migrate!")
                return False
            
            # Check if new tables exist
            cur.execute("SHOW TABLES LIKE 'khazana_programs'")
            if not cur.fetchone():
                print("❌ New table 'khazana_programs' not found. Run db.sql schema first!")
                return False
            
            print("\n✅ Found tables. Starting migration...\n")
            
            # Step 1: Rename old table for backup if needed
            if old_table_exists:
                print("Step 1: Backing up old table...")
                try:
                    cur.execute("RENAME TABLE khazana_lecture_uploads TO khazana_lecture_uploads_old")
                    conn.commit()
                    print("✅ Renamed khazana_lecture_uploads → khazana_lecture_uploads_old")
                except Exception as e:
                    if "already exists" in str(e).lower() or "doesn't exist" in str(e).lower():
                        print("⚠️  Table already renamed, continuing...")
                    else:
                        raise
            else:
                print("Step 1: Using existing backup table khazana_lecture_uploads_old...")
            
            # Step 2: Fetch all old data
            print("\nStep 2: Reading old data...")
            cur.execute("""
                SELECT DISTINCT
                    program_name,
                    subject_name,
                    teacher_name,
                    topic_name
                FROM khazana_lecture_uploads_old
                WHERE program_name IS NOT NULL
            """)
            old_data = cur.fetchall()
            print(f"✅ Found {len(old_data)} distinct program/subject/teacher/topic combinations")
            
            # Step 3: Populate programs
            print("\nStep 3: Populating khazana_programs...")
            programs = {}
            for row in old_data:
                program_name = row['program_name']
                if program_name and program_name not in programs:
                    program_id = make_slug(program_name)
                    cur.execute("""
                        INSERT IGNORE INTO khazana_programs (program_id, program_name)
                        VALUES (%s, %s)
                    """, (program_id, program_name))
                    programs[program_name] = program_id
            conn.commit()
            print(f"✅ Inserted {len(programs)} programs")
            
            # Step 4: Populate subjects
            print("\nStep 4: Populating khazana_subjects...")
            subjects = {}
            for row in old_data:
                subject_name = clean_subject_name(row['subject_name'])
                if subject_name and subject_name not in subjects:
                    subject_slug = make_slug(subject_name)
                    cur.execute("""
                        INSERT IGNORE INTO khazana_subjects (subject_name, subject_slug)
                        VALUES (%s, %s)
                    """, (subject_name, subject_slug))
                    subjects[subject_name] = subject_slug
            conn.commit()
            print(f"✅ Inserted {len(subjects)} subjects")
            
            # Step 5: Populate teachers
            print("\nStep 5: Populating khazana_teachers...")
            teachers = {}
            for row in old_data:
                teacher_name = row['teacher_name']
                if teacher_name and teacher_name not in teachers:
                    teacher_slug = make_slug(teacher_name)
                    cur.execute("""
                        INSERT IGNORE INTO khazana_teachers (teacher_name, teacher_slug)
                        VALUES (%s, %s)
                    """, (teacher_name, teacher_slug))
                    teachers[teacher_name] = teacher_slug
            conn.commit()
            print(f"✅ Inserted {len(teachers)} teachers")
            
            # Step 6: Populate topics
            print("\nStep 6: Populating khazana_topics...")
            topics_count = 0
            for row in old_data:
                program_name = row['program_name']
                subject_name = clean_subject_name(row['subject_name'])
                teacher_name = row['teacher_name']
                topic_name = row['topic_name']
                
                # Get program_id
                cur.execute("SELECT id FROM khazana_programs WHERE program_name = %s", (program_name,))
                prog_row = cur.fetchone()
                if not prog_row:
                    continue
                program_db_id = prog_row['id']
                
                # Get subject_id
                subject_db_id = None
                if subject_name:
                    cur.execute("SELECT id FROM khazana_subjects WHERE subject_name = %s", (subject_name,))
                    subj_row = cur.fetchone()
                    if subj_row:
                        subject_db_id = subj_row['id']
                
                # Get teacher_id
                teacher_db_id = None
                if teacher_name:
                    cur.execute("SELECT id FROM khazana_teachers WHERE teacher_name = %s", (teacher_name,))
                    teach_row = cur.fetchone()
                    if teach_row:
                        teacher_db_id = teach_row['id']
                
                # Create topic_id (use topic_name or generate from program)
                topic_id = topic_name if topic_name else f"{program_name}-default"
                
                cur.execute("""
                    INSERT IGNORE INTO khazana_topics (
                        program_id, subject_id, teacher_id, topic_id, topic_name
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """, (program_db_id, subject_db_id, teacher_db_id, topic_id, topic_name))
                topics_count += 1
            
            conn.commit()
            print(f"✅ Inserted {topics_count} topics")
            
            # Step 7: Migrate lectures
            print("\nStep 7: Migrating lectures...")
            cur.execute("SELECT * FROM khazana_lecture_uploads_old")
            old_lectures = cur.fetchall()
            
            lectures_migrated = 0
            lectures_failed = 0
            
            for lec in old_lectures:
                try:
                    # Find topic_id
                    program_name = lec['program_name']
                    topic_name = lec['topic_name'] or f"{program_name}-default"
                    
                    cur.execute("""
                        SELECT t.id
                        FROM khazana_topics t
                        JOIN khazana_programs p ON t.program_id = p.id
                        WHERE p.program_name = %s AND t.topic_id = %s
                    """, (program_name, topic_name))
                    topic_row = cur.fetchone()
                    
                    if not topic_row:
                        lectures_failed += 1
                        continue
                    
                    topic_db_id = topic_row['id']
                    
                    # Insert lecture
                    cur.execute("""
                        INSERT IGNORE INTO khazana_lectures (
                            topic_id, lecture_id, lecture_name, lecture_url, sub_topic_name,
                            thumbnail_url, thumbnail_mime, thumbnail_size, thumbnail_blob,
                            thumbnail_updated_at, ia_identifier, ia_url, status, server_id,
                            file_path, file_size, upload_bytes, upload_total, upload_percent,
                            telegram_chat_id, telegram_message_id, telegram_file_id,
                            error_text, created_at, updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s
                        )
                    """, (
                        topic_db_id, lec['lecture_id'], lec['lecture_name'], lec['lecture_url'],
                        lec['sub_topic_name'], lec['thumbnail_url'], lec['thumbnail_mime'],
                        lec['thumbnail_size'], lec['thumbnail_blob'], lec['thumbnail_updated_at'],
                        lec['ia_identifier'], lec['ia_url'], lec['status'], lec['server_id'],
                        lec['file_path'], lec['file_size'], lec['upload_bytes'],
                        lec['upload_total'], lec['upload_percent'], lec['telegram_chat_id'],
                        lec['telegram_message_id'], lec['telegram_file_id'], lec['error_text'],
                        lec['created_at'], lec['updated_at']
                    ))
                    lectures_migrated += 1
                    
                except Exception as e:
                    print(f"⚠️  Failed to migrate lecture {lec.get('lecture_id')}: {e}")
                    lectures_failed += 1
            
            conn.commit()
            print(f"✅ Migrated {lectures_migrated} lectures ({lectures_failed} failed)")
            
            print("\n" + "=" * 80)
            print("✅ Migration completed successfully!")
            print("=" * 80)
            print("\nSummary:")
            print(f"  • Programs: {len(programs)}")
            print(f"  • Subjects: {len(subjects)}")
            print(f"  • Teachers: {len(teachers)}")
            print(f"  • Topics: {topics_count}")
            print(f"  • Lectures: {lectures_migrated}")
            print("\n⚠️  Old data preserved in 'khazana_lecture_uploads_old' table")
            print("   You can drop it after verifying the migration.\n")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    success = migrate_khazana_schema()
    sys.exit(0 if success else 1)
