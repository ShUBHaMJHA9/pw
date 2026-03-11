#!/usr/bin/env python3
"""
Khazana database migration tool.
Migrates from legacy khazana_lecture_uploads(_old) into normalized tables plus
unified khazana_contents.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mainLogic.utils import mysql_logger


def clean_subject_name(subject_name):
    if not subject_name:
        return None
    clean = re.sub(r"\s+by\s+.*$", "", subject_name, flags=re.IGNORECASE).strip()
    return clean or None


def make_slug(text):
    if not text:
        return None
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def migrate_khazana_schema():
    print("=" * 80)
    print("Khazana Database Migration")
    print("=" * 80)

    conn = mysql_logger._connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES LIKE 'khazana_lecture_uploads'")
            old_exists = bool(cur.fetchone())
            cur.execute("SHOW TABLES LIKE 'khazana_lecture_uploads_old'")
            old_backup_exists = bool(cur.fetchone())

            if not old_exists and not old_backup_exists:
                print("No legacy khazana_lecture_uploads(_old) table found.")
                return False

            if old_exists and not old_backup_exists:
                cur.execute("RENAME TABLE khazana_lecture_uploads TO khazana_lecture_uploads_old")
                conn.commit()
                print("Renamed khazana_lecture_uploads -> khazana_lecture_uploads_old")

            cur.execute(
                """
                SELECT DISTINCT program_name, subject_name, teacher_name, topic_name
                FROM khazana_lecture_uploads_old
                WHERE program_name IS NOT NULL
                """
            )
            old_data = cur.fetchall()
            print(f"Found {len(old_data)} distinct topic combinations")

            for row in old_data:
                program_name = row.get("program_name")
                if not program_name:
                    continue
                cur.execute(
                    """
                    INSERT IGNORE INTO khazana_programs (program_id, program_name)
                    VALUES (%s, %s)
                    """,
                    (make_slug(program_name), program_name),
                )

                subject_name = clean_subject_name(row.get("subject_name"))
                if subject_name:
                    cur.execute(
                        """
                        INSERT IGNORE INTO khazana_subjects (subject_name, subject_slug)
                        VALUES (%s, %s)
                        """,
                        (subject_name, make_slug(subject_name)),
                    )

                teacher_name = row.get("teacher_name")
                if teacher_name:
                    cur.execute(
                        """
                        INSERT IGNORE INTO khazana_teachers (teacher_name, teacher_slug)
                        VALUES (%s, %s)
                        """,
                        (teacher_name, make_slug(teacher_name)),
                    )

            conn.commit()

            for row in old_data:
                program_name = row.get("program_name")
                topic_name = row.get("topic_name")
                topic_key = topic_name or f"{program_name}-default"
                subject_name = clean_subject_name(row.get("subject_name"))
                teacher_name = row.get("teacher_name")

                cur.execute("SELECT id FROM khazana_programs WHERE program_name=%s", (program_name,))
                p = cur.fetchone()
                if not p:
                    continue

                subject_id = None
                if subject_name:
                    cur.execute("SELECT id FROM khazana_subjects WHERE subject_name=%s", (subject_name,))
                    s = cur.fetchone()
                    subject_id = s["id"] if s else None

                teacher_id = None
                if teacher_name:
                    cur.execute("SELECT id FROM khazana_teachers WHERE teacher_name=%s", (teacher_name,))
                    t = cur.fetchone()
                    teacher_id = t["id"] if t else None

                cur.execute(
                    """
                    INSERT IGNORE INTO khazana_topics (program_id, subject_id, teacher_id, topic_id, topic_name)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (p["id"], subject_id, teacher_id, topic_key, topic_name),
                )

            conn.commit()

            cur.execute("SELECT * FROM khazana_lecture_uploads_old")
            lectures = cur.fetchall()
            migrated = 0
            failed = 0

            for lec in lectures:
                try:
                    program_name = lec.get("program_name")
                    topic_key = lec.get("topic_name") or f"{program_name}-default"

                    cur.execute(
                        """
                        SELECT t.id
                        FROM khazana_topics t
                        JOIN khazana_programs p ON t.program_id = p.id
                        WHERE p.program_name=%s AND t.topic_id=%s
                        """,
                        (program_name, topic_key),
                    )
                    topic = cur.fetchone()
                    if not topic:
                        failed += 1
                        continue

                    cur.execute(
                        """
                        INSERT IGNORE INTO khazana_contents (
                            topic_id,
                            content_type,
                            content_id,
                            content_name,
                            asset_kind,
                            source_url,
                            sub_topic_name,
                            thumbnail_url,
                            thumbnail_mime,
                            thumbnail_size,
                            thumbnail_blob,
                            thumbnail_updated_at,
                            ia_identifier,
                            ia_url,
                            status,
                            server_id,
                            file_path,
                            file_size,
                            upload_bytes,
                            upload_total,
                            upload_percent,
                            telegram_chat_id,
                            telegram_message_id,
                            telegram_file_id,
                            error_text,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            %s, 'lecture', %s, %s, '', %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        """,
                        (
                            topic["id"],
                            lec.get("lecture_id"),
                            lec.get("lecture_name"),
                            lec.get("lecture_url"),
                            lec.get("sub_topic_name"),
                            lec.get("thumbnail_url"),
                            lec.get("thumbnail_mime"),
                            lec.get("thumbnail_size"),
                            lec.get("thumbnail_blob"),
                            lec.get("thumbnail_updated_at"),
                            lec.get("ia_identifier"),
                            lec.get("ia_url"),
                            lec.get("status"),
                            lec.get("server_id"),
                            lec.get("file_path"),
                            lec.get("file_size"),
                            lec.get("upload_bytes"),
                            lec.get("upload_total"),
                            lec.get("upload_percent"),
                            lec.get("telegram_chat_id"),
                            lec.get("telegram_message_id"),
                            lec.get("telegram_file_id"),
                            lec.get("error_text"),
                            lec.get("created_at"),
                            lec.get("updated_at"),
                        ),
                    )
                    migrated += 1
                except Exception as exc:
                    print(f"Failed lecture {lec.get('lecture_id')}: {exc}")
                    failed += 1

            conn.commit()
            print(f"Migrated lectures: {migrated}, failed: {failed}")
            print("Legacy data remains in khazana_lecture_uploads_old for rollback.")
            return True
    except Exception as exc:
        print(f"Migration failed: {exc}")
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    ok = migrate_khazana_schema()
    sys.exit(0 if ok else 1)
