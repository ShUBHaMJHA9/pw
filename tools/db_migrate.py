import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mainLogic.utils import mysql_logger


load_dotenv()


def _exec(cur, sql):
    cur.execute(sql)


def migrate():
    mysql_logger.init(os.environ.get("PWDL_DB_URL"))
    mysql_logger.ensure_schema()

    conn = mysql_logger._connect()
    try:
        with conn.cursor() as cur:
            # Backfill subjects from lectures.
            _exec(
                cur,
                """
                INSERT INTO subjects (course_id, slug, name)
                SELECT DISTINCT
                    l.course_id,
                    COALESCE(NULLIF(l.subject_slug, ''), NULLIF(l.subject_name, '')) AS slug,
                    COALESCE(NULLIF(l.subject_name, ''), NULLIF(l.subject_slug, '')) AS name
                FROM lectures l
                WHERE l.course_id IS NOT NULL
                  AND (l.subject_slug IS NOT NULL OR l.subject_name IS NOT NULL)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name)
                """,
            )

            # Backfill chapters from lectures + subjects.
            _exec(
                cur,
                """
                INSERT INTO chapters (subject_id, name)
                SELECT DISTINCT
                    s.id,
                    l.chapter_name
                FROM lectures l
                JOIN subjects s
                  ON s.course_id = l.course_id
                 AND (s.slug = l.subject_slug OR s.name = l.subject_name)
                WHERE l.chapter_name IS NOT NULL AND l.chapter_name <> ''
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name)
                """,
            )

            # Link lectures to subjects/chapters.
            _exec(
                cur,
                """
                UPDATE lectures l
                JOIN subjects s
                  ON s.course_id = l.course_id
                 AND (s.slug = l.subject_slug OR s.name = l.subject_name)
                SET l.subject_id = s.id
                WHERE l.subject_id IS NULL
                """,
            )
            _exec(
                cur,
                """
                UPDATE lectures l
                JOIN subjects s
                  ON s.course_id = l.course_id
                 AND (s.slug = l.subject_slug OR s.name = l.subject_name)
                JOIN chapters ch
                  ON ch.subject_id = s.id
                 AND ch.name = l.chapter_name
                SET l.chapter_id = ch.id
                WHERE l.chapter_id IS NULL
                """,
            )

            # Backfill uploads from legacy lecture_jobs rows.
            _exec(
                cur,
                """
                INSERT IGNORE INTO lecture_uploads (
                    batch_id,
                    lecture_id,
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
                    error_text
                )
                SELECT
                    lj.batch_id,
                    lj.lecture_id,
                    CASE WHEN lj.telegram_message_id IS NOT NULL THEN 'done' ELSE lj.status END,
                    lj.server_id,
                    lj.file_path,
                    lj.file_size,
                    lj.upload_bytes,
                    lj.upload_total,
                    lj.upload_percent,
                    lj.telegram_chat_id,
                    lj.telegram_message_id,
                    lj.telegram_file_id,
                    lj.error_text
                FROM lecture_jobs lj
                """,
            )
            _exec(
                cur,
                """
                UPDATE lecture_uploads lu
                JOIN lecture_jobs lj
                  ON lu.batch_id = lj.batch_id AND lu.lecture_id = lj.lecture_id
                SET
                    lu.status = CASE WHEN lj.telegram_message_id IS NOT NULL THEN 'done' ELSE lj.status END,
                    lu.server_id = COALESCE(lj.server_id, lu.server_id),
                    lu.file_path = COALESCE(lj.file_path, lu.file_path),
                    lu.file_size = COALESCE(lj.file_size, lu.file_size),
                    lu.upload_bytes = COALESCE(lj.upload_bytes, lu.upload_bytes),
                    lu.upload_total = COALESCE(lj.upload_total, lu.upload_total),
                    lu.upload_percent = COALESCE(lj.upload_percent, lu.upload_percent),
                    lu.telegram_chat_id = COALESCE(lj.telegram_chat_id, lu.telegram_chat_id),
                    lu.telegram_message_id = COALESCE(lj.telegram_message_id, lu.telegram_message_id),
                    lu.telegram_file_id = COALESCE(lj.telegram_file_id, lu.telegram_file_id),
                    lu.error_text = COALESCE(lj.error_text, lu.error_text)
                """,
            )

            # Backfill backup_id from existing Telegram upload records.
            _exec(
                cur,
                """
                INSERT IGNORE INTO backup_id (
                    batch_id,
                    lecture_id,
                    platform,
                    channel_id,
                    message_id,
                    file_id
                )
                SELECT
                    lu.batch_id,
                    lu.lecture_id,
                    'telegram',
                    lu.telegram_chat_id,
                    lu.telegram_message_id,
                    lu.telegram_file_id
                FROM lecture_uploads lu
                WHERE lu.telegram_chat_id IS NOT NULL
                   OR lu.telegram_message_id IS NOT NULL
                   OR lu.telegram_file_id IS NOT NULL
                """,
            )
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
    print("Migration complete.")
