import os
from urllib.parse import urlparse, parse_qs

import pymysql
from dotenv import load_dotenv

load_dotenv()

_DB_URL = None


def init(db_url=None):
    global _DB_URL
    _DB_URL = db_url or os.environ.get("PWDL_DB_URL")
    if not _DB_URL:
        raise RuntimeError("PWDL_DB_URL not set and no --db-url provided")


def _parse_mysql_url(db_url):
    parsed = urlparse(db_url)
    if parsed.scheme not in ("mysql", "mysql+pymysql"):
        raise ValueError("Unsupported DB scheme")
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port or 3306
    database = parsed.path.lstrip("/") if parsed.path else None
    params = parse_qs(parsed.query)
    ssl_mode = params.get("ssl-mode", [None])[0]

    ssl = None
    if ssl_mode and ssl_mode.upper() != "DISABLED":
        # pymysql expects an ssl dict; empty dict enables default TLS
        ssl = {}

    return {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port,
        "ssl": ssl,
    }


def _connect():
    if not _DB_URL:
        init(None)
    cfg = _parse_mysql_url(_DB_URL)
    return pymysql.connect(
        host=cfg["host"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        port=cfg["port"],
        ssl=cfg["ssl"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def ensure_schema():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            def _try_execute(ddl):
                try:
                    cur.execute(ddl)
                except Exception:
                    pass

            def _constraint_exists(name):
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = %s
                    """,
                    (name,),
                )
                return cur.fetchone()["cnt"] > 0

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lecture_jobs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    batch_slug VARCHAR(128) NULL,
                    course_name VARCHAR(255) NULL,
                    lecture_id VARCHAR(128) NOT NULL,
                    subject_slug VARCHAR(128) NULL,
                    subject_name VARCHAR(255) NULL,
                    chapter_name VARCHAR(255) NULL,
                    lecture_name VARCHAR(255) NULL,
                    start_time VARCHAR(64) NULL,
                    teacher_ids TEXT NULL,
                    teacher_names TEXT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    server_id VARCHAR(128) NULL,
                    file_path TEXT NULL,
                    file_size BIGINT NULL,
                    upload_bytes BIGINT NULL,
                    upload_total BIGINT NULL,
                    upload_percent FLOAT NULL,
                    telegram_chat_id VARCHAR(128) NULL,
                    telegram_message_id VARCHAR(128) NULL,
                    telegram_file_id VARCHAR(255) NULL,
                    error_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_batch_lecture (batch_id, lecture_id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS courses (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    batch_slug VARCHAR(128) NULL,
                    name VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_course_batch (batch_id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS subjects (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    course_id BIGINT NOT NULL,
                    slug VARCHAR(128) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_subject_course_slug (course_id, slug),
                    CONSTRAINT fk_subject_course FOREIGN KEY (course_id) REFERENCES courses(id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chapters (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    subject_id BIGINT NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_chapter_subject_name (subject_id, name),
                    CONSTRAINT fk_chapter_subject FOREIGN KEY (subject_id) REFERENCES subjects(id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS teachers (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    teacher_key VARCHAR(255) NOT NULL,
                    teacher_id VARCHAR(128) NULL,
                    name VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_teacher_key (teacher_key)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lectures (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    lecture_id VARCHAR(128) NOT NULL,
                    course_id BIGINT NULL,
                    subject_id BIGINT NULL,
                    chapter_id BIGINT NULL,
                    subject_slug VARCHAR(128) NULL,
                    subject_name VARCHAR(255) NULL,
                    chapter_name VARCHAR(255) NULL,
                    lecture_name VARCHAR(255) NULL,
                    start_time VARCHAR(64) NULL,
                    chapter_total INT NULL,
                    display_order INT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_lecture_batch (batch_id, lecture_id),
                    KEY idx_lecture_course (course_id),
                    KEY idx_lecture_subject (subject_id),
                    KEY idx_lecture_chapter (chapter_id),
                    CONSTRAINT fk_lecture_course FOREIGN KEY (course_id) REFERENCES courses(id),
                    CONSTRAINT fk_lecture_subject FOREIGN KEY (subject_id) REFERENCES subjects(id),
                    CONSTRAINT fk_lecture_chapter FOREIGN KEY (chapter_id) REFERENCES chapters(id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lecture_teachers (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    lecture_id VARCHAR(128) NOT NULL,
                    batch_id VARCHAR(128) NOT NULL,
                    teacher_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_lecture_teacher (batch_id, lecture_id, teacher_id),
                    KEY idx_lecture_teacher (lecture_id, teacher_id),
                    CONSTRAINT fk_lecture_teachers_lecture FOREIGN KEY (batch_id, lecture_id)
                        REFERENCES lectures(batch_id, lecture_id),
                    CONSTRAINT fk_lecture_teachers_teacher FOREIGN KEY (teacher_id)
                        REFERENCES teachers(id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lecture_uploads (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    lecture_id VARCHAR(128) NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    server_id VARCHAR(128) NULL,
                    file_path TEXT NULL,
                    file_size BIGINT NULL,
                    upload_bytes BIGINT NULL,
                    upload_total BIGINT NULL,
                    upload_percent FLOAT NULL,
                    telegram_chat_id VARCHAR(128) NULL,
                    telegram_message_id VARCHAR(128) NULL,
                    telegram_file_id VARCHAR(255) NULL,
                    error_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_upload_lecture (batch_id, lecture_id),
                    CONSTRAINT fk_upload_lecture FOREIGN KEY (batch_id, lecture_id)
                        REFERENCES lectures(batch_id, lecture_id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dpp_backups (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    lecture_id VARCHAR(128) NOT NULL,
                    kind VARCHAR(64) NULL,
                    file_path TEXT NULL,
                    file_size BIGINT NULL,
                    telegram_chat_id VARCHAR(128) NULL,
                    telegram_message_id VARCHAR(128) NULL,
                    telegram_file_id VARCHAR(255) NULL,
                    metadata JSON NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    error_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_dpp (batch_id, lecture_id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS backup_id (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    lecture_id VARCHAR(128) NOT NULL,
                    platform VARCHAR(64) NOT NULL DEFAULT 'telegram',
                    channel_id VARCHAR(128) NULL,
                    message_id VARCHAR(128) NULL,
                    file_id VARCHAR(255) NULL,
                    metadata JSON NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_backup_target (batch_id, lecture_id, platform, channel_id)
                ) ENGINE=InnoDB;
                """
            )
            # Add new columns if table already existed
            columns = {
                "batch_slug": "ALTER TABLE lecture_jobs ADD COLUMN batch_slug VARCHAR(128) NULL",
                "course_name": "ALTER TABLE lecture_jobs ADD COLUMN course_name VARCHAR(255) NULL",
                "subject_name": "ALTER TABLE lecture_jobs ADD COLUMN subject_name VARCHAR(255) NULL",
                "teacher_ids": "ALTER TABLE lecture_jobs ADD COLUMN teacher_ids TEXT NULL",
                "teacher_names": "ALTER TABLE lecture_jobs ADD COLUMN teacher_names TEXT NULL",
                "upload_bytes": "ALTER TABLE lecture_jobs ADD COLUMN upload_bytes BIGINT NULL",
                "upload_total": "ALTER TABLE lecture_jobs ADD COLUMN upload_total BIGINT NULL",
                "upload_percent": "ALTER TABLE lecture_jobs ADD COLUMN upload_percent FLOAT NULL",
                "telegram_chat_id": "ALTER TABLE lecture_jobs ADD COLUMN telegram_chat_id VARCHAR(128) NULL",
                "telegram_message_id": "ALTER TABLE lecture_jobs ADD COLUMN telegram_message_id VARCHAR(128) NULL",
                "telegram_file_id": "ALTER TABLE lecture_jobs ADD COLUMN telegram_file_id VARCHAR(255) NULL",
            }
            for column_name, ddl in columns.items():
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lecture_jobs' AND COLUMN_NAME = %s
                    """,
                    (column_name,),
                )
                if cur.fetchone()["cnt"] == 0:
                    cur.execute(ddl)
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lecture_teachers'
                AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute(
                    "ALTER TABLE lecture_teachers ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST"
                )
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = 'subject_name'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lectures ADD COLUMN subject_name VARCHAR(255) NULL")
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = 'subject_id'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lectures ADD COLUMN subject_id BIGINT NULL")
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = 'chapter_id'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lectures ADD COLUMN chapter_id BIGINT NULL")
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = 'chapter_total'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lectures ADD COLUMN chapter_total INT NULL")
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = 'display_order'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lectures ADD COLUMN display_order INT NULL")

            _try_execute(
                "CREATE INDEX idx_lecture_subject ON lectures (subject_id)"
            )
            _try_execute(
                "CREATE INDEX idx_lecture_chapter ON lectures (chapter_id)"
            )
            _try_execute(
                "CREATE INDEX idx_lecture_course ON lectures (course_id)"
            )

            if not _constraint_exists("fk_lecture_course"):
                _try_execute(
                    "ALTER TABLE lectures ADD CONSTRAINT fk_lecture_course FOREIGN KEY (course_id) REFERENCES courses(id)"
                )
            if not _constraint_exists("fk_lecture_subject"):
                _try_execute(
                    "ALTER TABLE lectures ADD CONSTRAINT fk_lecture_subject FOREIGN KEY (subject_id) REFERENCES subjects(id)"
                )
            if not _constraint_exists("fk_lecture_chapter"):
                _try_execute(
                    "ALTER TABLE lectures ADD CONSTRAINT fk_lecture_chapter FOREIGN KEY (chapter_id) REFERENCES chapters(id)"
                )
            if not _constraint_exists("fk_lecture_teachers_lecture"):
                _try_execute(
                    "ALTER TABLE lecture_teachers ADD CONSTRAINT fk_lecture_teachers_lecture FOREIGN KEY (batch_id, lecture_id) REFERENCES lectures(batch_id, lecture_id)"
                )
            if not _constraint_exists("fk_lecture_teachers_teacher"):
                _try_execute(
                    "ALTER TABLE lecture_teachers ADD CONSTRAINT fk_lecture_teachers_teacher FOREIGN KEY (teacher_id) REFERENCES teachers(id)"
                )
            if not _constraint_exists("fk_upload_lecture"):
                _try_execute(
                    "ALTER TABLE lecture_uploads ADD CONSTRAINT fk_upload_lecture FOREIGN KEY (batch_id, lecture_id) REFERENCES lectures(batch_id, lecture_id)"
                )
    finally:
        conn.close()


def upsert_course(batch_id, batch_slug=None, course_name=None):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO courses (batch_id, batch_slug, name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    batch_slug=VALUES(batch_slug),
                    name=VALUES(name)
                """,
                (batch_id, batch_slug, course_name),
            )
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


def upsert_subject(course_id, subject_slug=None, subject_name=None):
    if not course_id or (not subject_slug and not subject_name):
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO subjects (course_id, slug, name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    name=VALUES(name)
                """,
                (course_id, subject_slug or subject_name, subject_name or subject_slug),
            )
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


def upsert_chapter(subject_id, chapter_name=None):
    if not subject_id or not chapter_name:
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chapters (subject_id, name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    name=VALUES(name)
                """,
                (subject_id, chapter_name),
            )
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


def upsert_teacher(teacher_id=None, teacher_name=None):
    if not teacher_id and not teacher_name:
        return None
    teacher_key = str(teacher_id) if teacher_id else str(teacher_name)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teachers (teacher_key, teacher_id, name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    teacher_id=VALUES(teacher_id),
                    name=VALUES(name)
                """,
                (teacher_key, teacher_id, teacher_name),
            )
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


def upsert_lecture(
    batch_id,
    lecture_id,
    subject_slug=None,
    subject_name=None,
    chapter_name=None,
    lecture_name=None,
    start_time=None,
    course_id=None,
    subject_id=None,
    chapter_id=None,
    display_order=None,
    chapter_total=None,
):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lectures (
                    batch_id,
                    lecture_id,
                    course_id,
                    subject_id,
                    chapter_id,
                    subject_slug,
                    subject_name,
                    chapter_name,
                    lecture_name,
                    start_time,
                    display_order,
                    chapter_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    course_id=VALUES(course_id),
                    subject_id=VALUES(subject_id),
                    chapter_id=VALUES(chapter_id),
                    subject_slug=VALUES(subject_slug),
                    subject_name=VALUES(subject_name),
                    chapter_name=VALUES(chapter_name),
                    lecture_name=VALUES(lecture_name),
                    start_time=VALUES(start_time),
                    display_order=VALUES(display_order),
                    chapter_total=VALUES(chapter_total)
                """,
                (
                    batch_id,
                    lecture_id,
                    course_id,
                    subject_id,
                    chapter_id,
                    subject_slug,
                    subject_name,
                    chapter_name,
                    lecture_name,
                    start_time,
                    display_order,
                    chapter_total,
                ),
            )
    finally:
        conn.close()


def link_lecture_teacher(batch_id, lecture_id, teacher_row_id):
    if not teacher_row_id:
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT IGNORE INTO lecture_teachers (batch_id, lecture_id, teacher_id)
                VALUES (%s, %s, %s)
                """,
                (batch_id, lecture_id, teacher_row_id),
            )
    finally:
        conn.close()


def _ensure_upload_row(batch_id, lecture_id, status=None, server_id=None):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lecture_uploads (batch_id, lecture_id, status, server_id)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status=COALESCE(VALUES(status), status),
                    server_id=COALESCE(VALUES(server_id), server_id)
                """,
                (batch_id, lecture_id, status or "pending", server_id),
            )
    finally:
        conn.close()


def reserve_lecture(
    batch_id,
    lecture_id,
    subject_slug,
    subject_name,
    chapter_name,
    lecture_name,
    start_time,
    server_id,
    lock_ttl_min,
    batch_slug=None,
    course_name=None,
    teacher_ids=None,
    teacher_names=None,
):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lecture_jobs (
                    batch_id,
                    batch_slug,
                    course_name,
                    lecture_id,
                    subject_slug,
                    subject_name,
                    chapter_name,
                    lecture_name,
                    start_time,
                    teacher_ids,
                    teacher_names,
                    status,
                    server_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                ON DUPLICATE KEY UPDATE
                    batch_slug=VALUES(batch_slug),
                    course_name=VALUES(course_name),
                    subject_slug=VALUES(subject_slug),
                    subject_name=VALUES(subject_name),
                    chapter_name=VALUES(chapter_name),
                    lecture_name=VALUES(lecture_name),
                    start_time=VALUES(start_time),
                    teacher_ids=VALUES(teacher_ids),
                    teacher_names=VALUES(teacher_names)
                """,
                (
                    batch_id,
                    batch_slug,
                    course_name,
                    lecture_id,
                    subject_slug,
                    subject_name,
                    chapter_name,
                    lecture_name,
                    start_time,
                    teacher_ids,
                    teacher_names,
                    server_id,
                ),
            )

            cur.execute(
                """
                UPDATE lecture_jobs
                SET status='downloading', server_id=%s, error_text=NULL
                WHERE batch_id=%s AND lecture_id=%s
                AND (
                    status IN ('pending', 'failed')
                    OR (status IN ('downloading', 'uploading') AND updated_at < (NOW() - INTERVAL %s MINUTE))
                )
                """,
                (server_id, batch_id, lecture_id, lock_ttl_min),
            )
            return cur.rowcount == 1
    finally:
        conn.close()


def _maybe_upsert_backup_id(cur, batch_id, lecture_id, platform, channel_id=None, message_id=None, file_id=None, metadata=None):
    if not (channel_id or message_id or file_id):
        return
    cur.execute(
        """
        INSERT INTO backup_id (
            batch_id, lecture_id, platform, channel_id, message_id, file_id, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            message_id=COALESCE(VALUES(message_id), message_id),
            file_id=COALESCE(VALUES(file_id), file_id),
            metadata=COALESCE(VALUES(metadata), metadata)
        """,
        (
            batch_id,
            lecture_id,
            platform,
            channel_id,
            message_id,
            file_id,
            metadata,
        ),
    )


def upsert_backup_id(batch_id, lecture_id, platform="telegram", channel_id=None, message_id=None, file_id=None, metadata=None):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            _maybe_upsert_backup_id(
                cur,
                batch_id,
                lecture_id,
                platform=platform,
                channel_id=channel_id,
                message_id=message_id,
                file_id=file_id,
                metadata=metadata,
            )
    finally:
        conn.close()


def mark_status(batch_id, lecture_id, status, file_path=None, file_size=None, error=None, telegram_chat_id=None, telegram_message_id=None, telegram_file_id=None):
    _ensure_upload_row(batch_id, lecture_id, status=status)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE lecture_jobs
                SET status=%s, error_text=%s, telegram_chat_id=COALESCE(%s, telegram_chat_id), telegram_message_id=COALESCE(%s, telegram_message_id), telegram_file_id=COALESCE(%s, telegram_file_id)
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (status, error, telegram_chat_id, telegram_message_id, telegram_file_id, batch_id, lecture_id),
            )
            cur.execute(
                """
                UPDATE lecture_uploads
                SET status=%s,
                    file_path=%s,
                    file_size=%s,
                    error_text=%s,
                    telegram_chat_id=%s,
                    telegram_message_id=%s,
                    telegram_file_id=%s
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (status, file_path, file_size, error, telegram_chat_id, telegram_message_id, telegram_file_id, batch_id, lecture_id),
            )
            _maybe_upsert_backup_id(
                cur,
                batch_id,
                lecture_id,
                platform="telegram",
                channel_id=telegram_chat_id,
                message_id=telegram_message_id,
                file_id=telegram_file_id,
            )
    finally:
        conn.close()


def mark_progress(batch_id, lecture_id, bytes_sent, bytes_total, percent=None, server_id=None):
    _ensure_upload_row(batch_id, lecture_id, status="uploading", server_id=server_id)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE lecture_jobs
                SET status='uploading',
                    server_id=COALESCE(%s, server_id),
                    error_text=NULL
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (server_id, batch_id, lecture_id),
            )
            cur.execute(
                """
                UPDATE lecture_uploads
                SET status='uploading',
                    upload_bytes=%s,
                    upload_total=%s,
                    upload_percent=%s,
                    server_id=COALESCE(%s, server_id),
                    error_text=NULL
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (bytes_sent, bytes_total, percent, server_id, batch_id, lecture_id),
            )
    finally:
        conn.close()


def is_upload_done(batch_id, lecture_id):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, telegram_message_id
                FROM lecture_uploads
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (batch_id, lecture_id),
            )
            row = cur.fetchone()
            if row and (row.get("status") == "done" or row.get("telegram_message_id") or row.get("telegram_file_id")):
                return True
            cur.execute(
                """
                SELECT status, telegram_chat_id, telegram_message_id
                FROM lecture_jobs
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (batch_id, lecture_id),
            )
            job_row = cur.fetchone()
            if job_row and (job_row.get("status") == "done" or job_row.get("telegram_message_id") or job_row.get("telegram_file_id")):
                cur.execute(
                    """
                    INSERT INTO lecture_uploads (batch_id, lecture_id, status, telegram_chat_id, telegram_message_id)
                    VALUES (%s, %s, 'done', %s, %s)
                    ON DUPLICATE KEY UPDATE
                        status='done',
                        telegram_chat_id=COALESCE(VALUES(telegram_chat_id), telegram_chat_id),
                        telegram_message_id=COALESCE(VALUES(telegram_message_id), telegram_message_id),
                        telegram_file_id=COALESCE(VALUES(telegram_file_id), telegram_file_id)
                    """,
                    (
                        batch_id,
                        lecture_id,
                        job_row.get("telegram_chat_id"),
                        job_row.get("telegram_message_id"),
                        job_row.get("telegram_file_id"),
                    ),
                )
                return True
            return False
    finally:
        conn.close()


def get_caption_payload(batch_id, lecture_id):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.name AS course_name,
                    s.slug AS subject_slug,
                    s.name AS subject_name,
                    ch.name AS chapter_name,
                    l.lecture_name,
                    l.start_time,
                    GROUP_CONCAT(t.name ORDER BY t.name SEPARATOR ', ') AS teacher_names
                FROM lectures l
                LEFT JOIN courses c ON c.id = l.course_id
                LEFT JOIN subjects s ON s.id = l.subject_id
                LEFT JOIN chapters ch ON ch.id = l.chapter_id
                LEFT JOIN lecture_teachers lt ON lt.batch_id = l.batch_id AND lt.lecture_id = l.lecture_id
                LEFT JOIN teachers t ON t.id = lt.teacher_id
                WHERE l.batch_id = %s AND l.lecture_id = %s
                GROUP BY l.batch_id, l.lecture_id, c.name, s.slug, s.name, ch.name, l.lecture_name, l.start_time
                """,
                (batch_id, lecture_id),
            )
            return cur.fetchone()
    finally:
        conn.close()


def get_recorded_file_path(batch_id, lecture_id):
    """Return a recorded local file path for a lecture if present in upload/job records.

    Checks `lecture_uploads` first, then falls back to `lecture_jobs`.
    Returns None if no file path is recorded.
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT file_path FROM lecture_uploads WHERE batch_id=%s AND lecture_id=%s
                """,
                (batch_id, lecture_id),
            )
            row = cur.fetchone()
            if row and row.get("file_path"):
                return row.get("file_path")
            cur.execute(
                """
                SELECT file_path FROM lecture_jobs WHERE batch_id=%s AND lecture_id=%s
                """,
                (batch_id, lecture_id),
            )
            row2 = cur.fetchone()
            if row2 and row2.get("file_path"):
                return row2.get("file_path")
            return None
    finally:
        conn.close()


def upsert_dpp_backup(batch_id, lecture_id, kind=None, file_path=None, file_size=None, telegram_chat_id=None, telegram_message_id=None, telegram_file_id=None, metadata=None, status=None, error=None):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dpp_backups (
                    batch_id, lecture_id, kind, file_path, file_size, telegram_chat_id, telegram_message_id, telegram_file_id, metadata, status, error_text
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, 'pending'), %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    kind=VALUES(kind),
                    file_path=VALUES(file_path),
                    file_size=VALUES(file_size),
                    telegram_chat_id=COALESCE(VALUES(telegram_chat_id), telegram_chat_id),
                    telegram_message_id=COALESCE(VALUES(telegram_message_id), telegram_message_id),
                    telegram_file_id=COALESCE(VALUES(telegram_file_id), telegram_file_id),
                    metadata=COALESCE(VALUES(metadata), metadata),
                    status=COALESCE(VALUES(status), status),
                    error_text=VALUES(error_text)
                """,
                (
                    batch_id,
                    lecture_id,
                    kind,
                    file_path,
                    file_size,
                    telegram_chat_id,
                    telegram_message_id,
                    telegram_file_id,
                    metadata,
                    status,
                    error,
                ),
            )
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


def get_dpp_backup(batch_id, lecture_id):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM dpp_backups WHERE batch_id=%s AND lecture_id=%s",
                (batch_id, lecture_id),
            )
            return cur.fetchone()
    finally:
        conn.close()
