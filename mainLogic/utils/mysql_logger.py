import os
from urllib.parse import urlparse, parse_qs

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
    try:
        import pymysql
    except Exception as e:
        raise RuntimeError("pymysql is not installed; DB logging is unavailable") from e

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
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_key VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NULL,
                    username VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_user_key (user_key)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lecture_jobs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    batch_slug VARCHAR(128) NULL,
                    course_name VARCHAR(255) NULL,
                    user_id BIGINT NULL,
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
                    user_id BIGINT NULL,
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
                    user_id BIGINT NULL,
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
                    ia_identifier VARCHAR(255) NULL,
                    ia_url TEXT NULL,
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
            # Create normalized Khazana tables
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS khazana_programs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    program_id VARCHAR(128) NOT NULL,
                    program_name VARCHAR(255) NOT NULL,
                    thumbnail_url TEXT NULL,
                    thumbnail_mime VARCHAR(64) NULL,
                    thumbnail_size BIGINT NULL,
                    thumbnail_blob LONGBLOB NULL,
                    thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_khazana_program (program_id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS khazana_subjects (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    subject_name VARCHAR(255) NOT NULL,
                    subject_slug VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_khazana_subject_name (subject_name)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS khazana_teachers (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    teacher_name VARCHAR(255) NOT NULL,
                    teacher_slug VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_khazana_teacher_name (teacher_name)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS khazana_topics (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    program_id BIGINT NOT NULL,
                    subject_id BIGINT NULL,
                    teacher_id BIGINT NULL,
                    topic_id VARCHAR(128) NOT NULL,
                    topic_name VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_khazana_topic (program_id, topic_id),
                    KEY idx_khazana_topic_subject (subject_id),
                    KEY idx_khazana_topic_teacher (teacher_id)
                ) ENGINE=InnoDB;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS khazana_contents (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    topic_id BIGINT NOT NULL,
                    content_type VARCHAR(32) NOT NULL,
                    content_id VARCHAR(128) NOT NULL,
                    content_name VARCHAR(255) NULL,
                    asset_kind VARCHAR(64) NOT NULL DEFAULT '',
                    source_url TEXT NULL,
                    sub_topic_name VARCHAR(255) NULL,
                    thumbnail_url TEXT NULL,
                    thumbnail_mime VARCHAR(64) NULL,
                    thumbnail_size BIGINT NULL,
                    thumbnail_blob LONGBLOB NULL,
                    thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL,
                    ia_identifier VARCHAR(255) NULL,
                    ia_url TEXT NULL,
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
                    UNIQUE KEY uniq_khazana_content (topic_id, content_type, content_id, asset_kind),
                    KEY idx_khazana_content_status (status),
                    KEY idx_khazana_content_type (content_type),
                    KEY idx_khazana_content_ia (ia_identifier)
                ) ENGINE=InnoDB;
                """
            )
            
            # Add foreign key constraints for khazana tables
            if not _constraint_exists("fk_khazana_topic_program"):
                _try_execute(
                    "ALTER TABLE khazana_topics ADD CONSTRAINT fk_khazana_topic_program FOREIGN KEY (program_id) REFERENCES khazana_programs(id) ON DELETE CASCADE"
                )
            if not _constraint_exists("fk_khazana_topic_subject"):
                _try_execute(
                    "ALTER TABLE khazana_topics ADD CONSTRAINT fk_khazana_topic_subject FOREIGN KEY (subject_id) REFERENCES khazana_subjects(id) ON DELETE SET NULL"
                )
            if not _constraint_exists("fk_khazana_topic_teacher"):
                _try_execute(
                    "ALTER TABLE khazana_topics ADD CONSTRAINT fk_khazana_topic_teacher FOREIGN KEY (teacher_id) REFERENCES khazana_teachers(id) ON DELETE SET NULL"
                )
            if not _constraint_exists("fk_khazana_content_topic"):
                _try_execute(
                    "ALTER TABLE khazana_contents ADD CONSTRAINT fk_khazana_content_topic FOREIGN KEY (topic_id) REFERENCES khazana_topics(id) ON DELETE CASCADE"
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
            # Professional test management structure
            # Main tests table: batch_id + test_id as compound key (like lectures)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tests (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    test_id VARCHAR(128) NOT NULL,
                    test_name VARCHAR(255) NULL,
                    test_type VARCHAR(64) NULL,
                    test_template VARCHAR(64) NULL,
                    language_code VARCHAR(32) NULL,
                    sections_json JSON NULL,
                    difficulty_levels_json JSON NULL,
                    source_url TEXT NULL,
                    thumbnail_url TEXT NULL,
                    thumbnail_mime VARCHAR(64) NULL,
                    thumbnail_size BIGINT NULL,
                    thumbnail_blob LONGBLOB NULL,
                    thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL,
                    ia_identifier VARCHAR(255) NULL,
                    ia_url TEXT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    error_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_test (batch_id, test_id),
                    KEY idx_test_status (status),
                    KEY idx_test_batch (batch_id),
                    KEY idx_test_ia (ia_identifier)
                ) ENGINE=InnoDB;
                """
            )
            # Questions: child table linked to tests
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS test_questions (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    test_id VARCHAR(128) NOT NULL,
                    question_id VARCHAR(128) NOT NULL,
                    question_number INT NULL,
                    question_type VARCHAR(64) NULL,
                    positive_marks DECIMAL(10,4) NULL,
                    negative_marks DECIMAL(10,4) NULL,
                    difficulty_level INT NULL,
                    section_id VARCHAR(128) NULL,
                    subject_id VARCHAR(128) NULL,
                    chapter_id VARCHAR(128) NULL,
                    topic_id VARCHAR(128) NULL,
                    sub_topic_id VARCHAR(128) NULL,
                    qbg_id VARCHAR(128) NULL,
                    qbg_subject_id VARCHAR(128) NULL,
                    qbg_chapter_id VARCHAR(128) NULL,
                    qbg_topic_id VARCHAR(128) NULL,
                    correct_option_ids_json JSON NULL,
                    correct_answer_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_question (batch_id, test_id, question_id),
                    KEY idx_question_number (test_id, question_number)
                ) ENGINE=InnoDB;
                """
            )
            # Question options: child table linked to questions
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS test_options (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    test_id VARCHAR(128) NOT NULL,
                    question_id VARCHAR(128) NOT NULL,
                    option_id VARCHAR(128) NOT NULL,
                    option_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_option (batch_id, test_id, question_id, option_id),
                    KEY idx_option_question (test_id, question_id)
                ) ENGINE=InnoDB;
                """
            )
            # Test assets: images and videos for questions/solutions
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS test_assets (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(128) NOT NULL,
                    test_id VARCHAR(128) NOT NULL,
                    question_id VARCHAR(128) NULL,
                    asset_kind VARCHAR(64) NOT NULL,
                    asset_type VARCHAR(32) NULL,
                    source_url TEXT NULL,
                    source_key VARCHAR(255) NULL,
                    file_path TEXT NULL,
                    file_size BIGINT NULL,
                    file_mime VARCHAR(64) NULL,
                    storage_provider VARCHAR(64) NULL,
                    storage_id VARCHAR(255) NULL,
                    storage_url TEXT NULL,
                    ia_identifier VARCHAR(255) NULL,
                    ia_url TEXT NULL,
                    youtube_id VARCHAR(64) NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    error_text TEXT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_asset (batch_id, test_id, question_id, asset_kind, source_key),
                    KEY idx_asset_ia (ia_identifier),
                    KEY idx_asset_status (status),
                    KEY idx_asset_question (test_id, question_id)
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
                "user_id": "ALTER TABLE lecture_jobs ADD COLUMN user_id BIGINT NULL",
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

            for column_name, ddl in (
                ("thumbnail_url", "ALTER TABLE lectures ADD COLUMN thumbnail_url TEXT NULL"),
                ("thumbnail_mime", "ALTER TABLE lectures ADD COLUMN thumbnail_mime VARCHAR(64) NULL"),
                ("thumbnail_size", "ALTER TABLE lectures ADD COLUMN thumbnail_size BIGINT NULL"),
                ("thumbnail_blob", "ALTER TABLE lectures ADD COLUMN thumbnail_blob LONGBLOB NULL"),
                ("thumbnail_updated_at", "ALTER TABLE lectures ADD COLUMN thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL"),
            ):
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = %s
                    """,
                    (column_name,),
                )
                if cur.fetchone()["cnt"] == 0:
                    cur.execute(ddl)

            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lectures' AND COLUMN_NAME = 'user_id'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lectures ADD COLUMN user_id BIGINT NULL")

            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'courses' AND COLUMN_NAME = 'user_id'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE courses ADD COLUMN user_id BIGINT NULL")

            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lecture_uploads' AND COLUMN_NAME = 'ia_identifier'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lecture_uploads ADD COLUMN ia_identifier VARCHAR(255) NULL")
            cur.execute(
                """
                SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lecture_uploads' AND COLUMN_NAME = 'ia_url'
                """
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute("ALTER TABLE lecture_uploads ADD COLUMN ia_url TEXT NULL")

            for column_name, ddl in (
                ("content_type", "ALTER TABLE khazana_contents ADD COLUMN content_type VARCHAR(32) NOT NULL DEFAULT 'lecture'"),
                ("content_id", "ALTER TABLE khazana_contents ADD COLUMN content_id VARCHAR(128) NULL"),
                ("content_name", "ALTER TABLE khazana_contents ADD COLUMN content_name VARCHAR(255) NULL"),
                ("asset_kind", "ALTER TABLE khazana_contents ADD COLUMN asset_kind VARCHAR(64) NOT NULL DEFAULT ''"),
                ("source_url", "ALTER TABLE khazana_contents ADD COLUMN source_url TEXT NULL"),
                ("thumbnail_url", "ALTER TABLE khazana_contents ADD COLUMN thumbnail_url TEXT NULL"),
                ("thumbnail_mime", "ALTER TABLE khazana_contents ADD COLUMN thumbnail_mime VARCHAR(64) NULL"),
                ("thumbnail_size", "ALTER TABLE khazana_contents ADD COLUMN thumbnail_size BIGINT NULL"),
                ("thumbnail_blob", "ALTER TABLE khazana_contents ADD COLUMN thumbnail_blob LONGBLOB NULL"),
                ("thumbnail_updated_at", "ALTER TABLE khazana_contents ADD COLUMN thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL"),
                ("ia_identifier", "ALTER TABLE khazana_contents ADD COLUMN ia_identifier VARCHAR(255) NULL"),
                ("ia_url", "ALTER TABLE khazana_contents ADD COLUMN ia_url TEXT NULL"),
                ("upload_bytes", "ALTER TABLE khazana_contents ADD COLUMN upload_bytes BIGINT NULL"),
                ("upload_total", "ALTER TABLE khazana_contents ADD COLUMN upload_total BIGINT NULL"),
                ("upload_percent", "ALTER TABLE khazana_contents ADD COLUMN upload_percent FLOAT NULL"),
                ("telegram_chat_id", "ALTER TABLE khazana_contents ADD COLUMN telegram_chat_id VARCHAR(128) NULL"),
                ("telegram_message_id", "ALTER TABLE khazana_contents ADD COLUMN telegram_message_id VARCHAR(128) NULL"),
                ("telegram_file_id", "ALTER TABLE khazana_contents ADD COLUMN telegram_file_id VARCHAR(255) NULL"),
            ):
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'khazana_contents' AND COLUMN_NAME = %s
                    """,
                    (column_name,),
                )
                if cur.fetchone()["cnt"] == 0:
                    cur.execute(ddl)

            for table_name, additions in (
                (
                    "test_series",
                    (
                        ("source_url", "ALTER TABLE test_series ADD COLUMN source_url TEXT NULL"),
                        ("language_code", "ALTER TABLE test_series ADD COLUMN language_code VARCHAR(32) NULL"),
                        ("sections_json", "ALTER TABLE test_series ADD COLUMN sections_json JSON NULL"),
                        ("difficulty_levels_json", "ALTER TABLE test_series ADD COLUMN difficulty_levels_json JSON NULL"),
                        ("status", "ALTER TABLE test_series ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'pending'"),
                        ("error_text", "ALTER TABLE test_series ADD COLUMN error_text TEXT NULL"),
                    ),
                ),
                (
                    "test_series_batch_links",
                    (
                        ("batch_slug", "ALTER TABLE test_series_batch_links ADD COLUMN batch_slug VARCHAR(128) NULL"),
                        ("batch_name", "ALTER TABLE test_series_batch_links ADD COLUMN batch_name VARCHAR(255) NULL"),
                        ("test_mapping_id", "ALTER TABLE test_series_batch_links ADD COLUMN test_mapping_id VARCHAR(128) NULL"),
                        ("status", "ALTER TABLE test_series_batch_links ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'pending'"),
                        ("error_text", "ALTER TABLE test_series_batch_links ADD COLUMN error_text TEXT NULL"),
                    ),
                ),
                (
                    "test_series_questions",
                    (
                        ("question_image_source_url", "ALTER TABLE test_series_questions ADD COLUMN question_image_source_url TEXT NULL"),
                        ("question_image_storage_provider", "ALTER TABLE test_series_questions ADD COLUMN question_image_storage_provider VARCHAR(64) NULL"),
                        ("question_image_storage_id", "ALTER TABLE test_series_questions ADD COLUMN question_image_storage_id VARCHAR(255) NULL"),
                        ("question_image_storage_url", "ALTER TABLE test_series_questions ADD COLUMN question_image_storage_url TEXT NULL"),
                        ("correct_option_ids_json", "ALTER TABLE test_series_questions ADD COLUMN correct_option_ids_json JSON NULL"),
                        ("correct_answer_text", "ALTER TABLE test_series_questions ADD COLUMN correct_answer_text TEXT NULL"),
                    ),
                ),
                (
                    "test_series_solution_assets",
                    (
                        ("youtube_id", "ALTER TABLE test_series_solution_assets ADD COLUMN youtube_id VARCHAR(64) NULL"),
                    ),
                ),
            ):
                for column_name, ddl in additions:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
                        """,
                        (table_name, column_name),
                    )
                    if cur.fetchone()["cnt"] == 0:
                        _try_execute(ddl)

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


def upsert_user(user_key=None, name=None, username=None):
    if not user_key:
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_key, name, username)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    name=VALUES(name),
                    username=VALUES(username)
                """,
                (user_key, name, username),
            )
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        conn.close()


def upsert_course(batch_id, batch_slug=None, course_name=None, user_id=None):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO courses (batch_id, batch_slug, name, user_id)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    id=LAST_INSERT_ID(id),
                    batch_slug=VALUES(batch_slug),
                    name=VALUES(name),
                    user_id=COALESCE(VALUES(user_id), user_id)
                """,
                (batch_id, batch_slug, course_name, user_id),
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
    user_id=None,
):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lectures (
                    batch_id,
                    lecture_id,
                    user_id,
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    user_id=COALESCE(VALUES(user_id), user_id),
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
                    user_id,
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
            # Ensure a corresponding lectures row exists so the FK on lecture_uploads
            # doesn't fail when an upload record is created before the lecture row.
            if batch_id and lecture_id:
                cur.execute(
                    """
                    INSERT IGNORE INTO lectures (batch_id, lecture_id)
                    VALUES (%s, %s)
                    """,
                    (batch_id, lecture_id),
                )
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
    user_id=None,
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
                    user_id,
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                ON DUPLICATE KEY UPDATE
                    batch_slug=VALUES(batch_slug),
                    course_name=VALUES(course_name),
                    user_id=COALESCE(VALUES(user_id), user_id),
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
                    user_id,
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
            if status == "failed":
                cur.execute(
                    """
                    UPDATE lecture_jobs
                    SET status=%s, error_text=%s, telegram_chat_id=NULL, telegram_message_id=NULL, telegram_file_id=NULL
                    WHERE batch_id=%s AND lecture_id=%s
                    """,
                    (status, error, batch_id, lecture_id),
                )
                cur.execute(
                    """
                    UPDATE lecture_uploads
                    SET status=%s,
                        file_path=%s,
                        file_size=%s,
                        error_text=%s,
                        telegram_chat_id=NULL,
                        telegram_message_id=NULL,
                        telegram_file_id=NULL
                    WHERE batch_id=%s AND lecture_id=%s
                    """,
                    (status, file_path, file_size, error, batch_id, lecture_id),
                )
            else:
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


def update_ia_upload(batch_id, lecture_id, ia_identifier=None, ia_url=None):
    _ensure_upload_row(batch_id, lecture_id, status=None)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE lecture_uploads
                SET ia_identifier=COALESCE(%s, ia_identifier),
                    ia_url=COALESCE(%s, ia_url)
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (ia_identifier, ia_url, batch_id, lecture_id),
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


def has_lecture_thumbnail(batch_id, lecture_id):
    if not (batch_id and lecture_id):
        return False
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT thumbnail_blob
                FROM lectures
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (batch_id, lecture_id),
            )
            row = cur.fetchone()
            blob = row.get("thumbnail_blob") if row else None
            return bool(blob)
    finally:
        conn.close()


def get_lecture_thumbnail_blob(batch_id, lecture_id):
    if not (batch_id and lecture_id):
        return None, None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT thumbnail_blob, thumbnail_mime
                FROM lectures
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (batch_id, lecture_id),
            )
            row = cur.fetchone()
            if not row:
                return None, None
            return row.get("thumbnail_blob"), row.get("thumbnail_mime")
    finally:
        conn.close()


def update_lecture_thumbnail(batch_id, lecture_id, thumbnail_blob, thumbnail_mime=None, thumbnail_url=None):
    if not (batch_id and lecture_id and thumbnail_blob):
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT IGNORE INTO lectures (batch_id, lecture_id)
                VALUES (%s, %s)
                """,
                (batch_id, lecture_id),
            )
            cur.execute(
                """
                UPDATE lectures
                SET thumbnail_blob=%s,
                    thumbnail_mime=%s,
                    thumbnail_size=%s,
                    thumbnail_url=COALESCE(%s, thumbnail_url),
                    thumbnail_updated_at=NOW()
                WHERE batch_id=%s AND lecture_id=%s
                """,
                (
                    thumbnail_blob,
                    thumbnail_mime,
                    len(thumbnail_blob),
                    thumbnail_url,
                    batch_id,
                    lecture_id,
                ),
            )
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


def upsert_khazana_lecture(
    program_name,
    lecture_id,
    topic_name=None,
    subject_name=None,
    teacher_name=None,
    sub_topic_name=None,
    lecture_name=None,
    lecture_url=None,
    thumbnail_blob=None,
    thumbnail_mime=None,
    thumbnail_url=None,
    thumbnail_size=None,
    ia_identifier=None,
    ia_url=None,
    status=None,
    server_id=None,
    file_path=None,
    file_size=None,
    upload_bytes=None,
    upload_total=None,
    upload_percent=None,
    telegram_chat_id=None,
    telegram_message_id=None,
    telegram_file_id=None,
    error=None,
):
    if not (program_name and lecture_id):
        return

    topic_key = _make_slug(topic_name) if topic_name else lecture_id
    return upsert_khazana_lecture_v2(
        program_name=program_name,
        topic_id=topic_key,
        lecture_id=lecture_id,
        topic_name=topic_name,
        subject_name=subject_name,
        teacher_name=teacher_name,
        sub_topic_name=sub_topic_name,
        lecture_name=lecture_name,
        lecture_url=lecture_url,
        thumbnail_blob=thumbnail_blob,
        thumbnail_mime=thumbnail_mime,
        thumbnail_url=thumbnail_url,
        thumbnail_size=thumbnail_size,
        ia_identifier=ia_identifier,
        ia_url=ia_url,
        status=status,
        server_id=server_id,
        file_path=file_path,
        file_size=file_size,
        upload_bytes=upload_bytes,
        upload_total=upload_total,
        upload_percent=upload_percent,
        telegram_chat_id=telegram_chat_id,
        telegram_message_id=telegram_message_id,
        telegram_file_id=telegram_file_id,
        error=error,
    )


def get_khazana_upload_status(program_name, lecture_id, topic_name=None):
    if not (program_name and lecture_id):
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            params = [program_name, lecture_id]
            topic_clause = ""
            if topic_name:
                topic_clause = " AND (t.topic_name=%s OR t.topic_id=%s)"
                params.extend([topic_name, topic_name])

            cur.execute(
                f"""
                SELECT
                    c.status,
                    c.file_path,
                    c.file_size,
                    c.ia_identifier,
                    c.ia_url,
                    s.subject_name,
                    teach.teacher_name,
                    t.topic_name,
                    c.sub_topic_name,
                    c.content_name AS lecture_name,
                    c.updated_at,
                    c.created_at
                FROM khazana_contents c
                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                LEFT JOIN khazana_subjects s ON t.subject_id = s.id
                LEFT JOIN khazana_teachers teach ON t.teacher_id = teach.id
                WHERE c.content_type='lecture'
                  AND p.program_name=%s
                  AND c.content_id=%s
                  {topic_clause}
                ORDER BY c.id DESC
                LIMIT 1
                """,
                tuple(params),
            )
            return cur.fetchone()
    finally:
        conn.close()


def list_khazana_lectures(
    program_name=None,
    status=None,
    subject_name=None,
    teacher_name=None,
    topic_name=None,
    limit=1000,
):
    """Return Khazana lectures in deterministic sequence for queue/retry workflows."""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            clauses = []
            params = []
            if program_name:
                clauses.append("p.program_name=%s")
                params.append(program_name)
            if status:
                clauses.append("c.status=%s")
                params.append(status)
            if subject_name:
                clauses.append("s.subject_name=%s")
                params.append(subject_name)
            if teacher_name:
                clauses.append("teach.teacher_name=%s")
                params.append(teacher_name)
            if topic_name:
                clauses.append("t.topic_name=%s")
                params.append(topic_name)

            where_sql = f"AND {' AND '.join(clauses)}" if clauses else ""
            safe_limit = max(1, min(int(limit or 1000), 10000))
            cur.execute(
                f"""
                SELECT
                    c.id,
                    p.program_name,
                    s.subject_name,
                    teach.teacher_name,
                    t.topic_name,
                    c.sub_topic_name,
                    c.content_id AS lecture_id,
                    c.content_name AS lecture_name,
                    c.source_url AS lecture_url,
                    c.ia_identifier,
                    c.ia_url,
                    c.status,
                    c.file_path,
                    c.file_size,
                    c.error_text,
                    c.created_at,
                    c.updated_at
                FROM khazana_contents c
                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                LEFT JOIN khazana_subjects s ON t.subject_id = s.id
                LEFT JOIN khazana_teachers teach ON t.teacher_id = teach.id
                WHERE c.content_type='lecture'
                {where_sql}
                ORDER BY c.created_at ASC, c.id ASC
                LIMIT {safe_limit}
                """,
                tuple(params),
            )
            return cur.fetchall() or []
    finally:
        conn.close()


def has_khazana_thumbnail(program_name, lecture_id, topic_name=None):
    if not (program_name and lecture_id):
        return False
    conn = _connect()
    try:
        with conn.cursor() as cur:
            params = [program_name, lecture_id]
            topic_clause = ""
            if topic_name:
                topic_clause = " AND (t.topic_name=%s OR t.topic_id=%s)"
                params.extend([topic_name, topic_name])

            cur.execute(
                f"""
                SELECT c.thumbnail_blob
                FROM khazana_contents c
                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                WHERE c.content_type='lecture'
                  AND p.program_name=%s
                  AND c.content_id=%s
                  {topic_clause}
                ORDER BY c.id DESC
                LIMIT 1
                """,
                tuple(params),
            )
            row = cur.fetchone()
            blob = row.get("thumbnail_blob") if row else None
            return bool(blob)
    finally:
        conn.close()


def upsert_khazana_asset(
    program_name,
    content_id,
    kind,
    content_name=None,
    file_url=None,
    file_path=None,
    file_size=None,
    ia_identifier=None,
    ia_url=None,
    status=None,
    server_id=None,
    subject_name=None,
    teacher_name=None,
    topic_name=None,
    sub_topic_name=None,
    error=None,
):
    if not (program_name and content_id and kind):
        return

    topic_key = _make_slug(topic_name) if topic_name else f"asset-{kind}"
    topic_db_id = get_or_create_khazana_topic(
        program_name=program_name,
        topic_id=topic_key,
        topic_name=topic_name,
        subject_name=subject_name,
        teacher_name=teacher_name,
    )
    if not topic_db_id:
        return

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO khazana_contents (
                    topic_id,
                    content_type,
                    content_id,
                    content_name,
                    asset_kind,
                    source_url,
                    sub_topic_name,
                    file_path,
                    file_size,
                    ia_identifier,
                    ia_url,
                    status,
                    server_id,
                    error_text
                )
                VALUES (
                                        %s,
                    'asset',
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    COALESCE(%s, 'pending'),
                    %s,
                    %s
                )
                ON DUPLICATE KEY UPDATE
                    content_name=COALESCE(VALUES(content_name), content_name),
                    source_url=COALESCE(VALUES(source_url), source_url),
                    sub_topic_name=COALESCE(VALUES(sub_topic_name), sub_topic_name),
                    file_path=COALESCE(VALUES(file_path), file_path),
                    file_size=COALESCE(VALUES(file_size), file_size),
                    ia_identifier=COALESCE(VALUES(ia_identifier), ia_identifier),
                    ia_url=COALESCE(VALUES(ia_url), ia_url),
                    status=COALESCE(VALUES(status), status),
                    server_id=COALESCE(VALUES(server_id), server_id),
                    error_text=COALESCE(VALUES(error_text), error_text)
                """,
                (
                    topic_db_id,
                    content_id,
                    content_name,
                    kind,
                    file_url,
                    sub_topic_name,
                    file_path,
                    file_size,
                    ia_identifier,
                    ia_url,
                    status,
                    server_id,
                    error,
                ),
            )
    finally:
        conn.close()


def get_khazana_asset_status(program_name, content_id, kind):
    if not (program_name and content_id and kind):
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.status,
                    c.file_path,
                    c.file_size,
                    c.ia_identifier,
                    c.ia_url
                FROM khazana_contents c
                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                WHERE c.content_type='asset'
                  AND p.program_name=%s
                  AND c.content_id=%s
                  AND c.asset_kind=%s
                """,
                (program_name, content_id, kind),
            )
            return cur.fetchone()
    finally:
        conn.close()


# ============================================================================
# Normalized Khazana Schema Functions (New)
# ============================================================================

import re


def _make_slug(text):
    """Convert text to URL-friendly slug"""
    if not text:
        return None
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')


def _clean_subject_name(subject_name):
    """Extract clean subject name by removing 'by Teacher' suffix"""
    if not subject_name:
        return None
    # Remove "by XYZ" pattern
    clean = re.sub(r'\s+by\s+.*$', '', subject_name, flags=re.IGNORECASE).strip()
    return clean if clean else None


def get_or_create_khazana_program(program_name, thumbnail_url=None, thumbnail_blob=None, thumbnail_mime=None, thumbnail_size=None):
    """
    Get or create a Khazana program (e.g., "JEE 2025")
    Returns: program database ID
    """
    if not program_name:
        return None
    
    program_id = _make_slug(program_name)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Try to get existing
            cur.execute(
                "SELECT id FROM khazana_programs WHERE program_id=%s",
                (program_id,)
            )
            row = cur.fetchone()
            if row:
                # Update thumbnail if provided
                if thumbnail_url or thumbnail_blob:
                    cur.execute("""
                        UPDATE khazana_programs
                        SET thumbnail_url=COALESCE(%s, thumbnail_url),
                            thumbnail_blob=COALESCE(%s, thumbnail_blob),
                            thumbnail_mime=COALESCE(%s, thumbnail_mime),
                            thumbnail_size=COALESCE(%s, thumbnail_size),
                            thumbnail_updated_at=CURRENT_TIMESTAMP
                        WHERE id=%s
                    """, (thumbnail_url, thumbnail_blob, thumbnail_mime, thumbnail_size, row['id']))
                    conn.commit()
                return row['id']
            
            # Insert new
            cur.execute("""
                INSERT INTO khazana_programs (
                    program_id, program_name, thumbnail_url, thumbnail_blob,
                    thumbnail_mime, thumbnail_size, thumbnail_updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    IF(%s IS NOT NULL OR %s IS NOT NULL, CURRENT_TIMESTAMP, NULL)
                )
            """, (
                program_id,
                program_name,
                thumbnail_url,
                thumbnail_blob,
                thumbnail_mime,
                thumbnail_size,
                thumbnail_url,
                thumbnail_blob,
            ))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row['id'] if row else None
    finally:
        conn.close()


def get_or_create_khazana_subject(subject_name):
    """
    Get or create a Khazana subject (e.g., "Data Structure")
    Automatically cleans subject names (removes "by Teacher" suffix)
    Returns: subject database ID
    """
    clean_name = _clean_subject_name(subject_name)
    if not clean_name:
        return None
    
    subject_slug = _make_slug(clean_name)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Try to get existing
            cur.execute(
                "SELECT id FROM khazana_subjects WHERE subject_name=%s",
                (clean_name,)
            )
            row = cur.fetchone()
            if row:
                return row['id']
            
            # Insert new
            cur.execute("""
                INSERT INTO khazana_subjects (subject_name, subject_slug)
                VALUES (%s, %s)
            """, (clean_name, subject_slug))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row['id'] if row else None
    finally:
        conn.close()


def get_or_create_khazana_teacher(teacher_name):
    """
    Get or create a Khazana teacher
    Returns: teacher database ID
    """
    if not teacher_name:
        return None

    teacher_name = str(teacher_name).strip()
    # Ignore generic chapter labels like "C Programming by".
    if not teacher_name or teacher_name.lower().endswith(" by"):
        return None
    
    teacher_slug = _make_slug(teacher_name)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Try to get existing
            cur.execute(
                "SELECT id FROM khazana_teachers WHERE teacher_name=%s",
                (teacher_name,)
            )
            row = cur.fetchone()
            if row:
                return row['id']
            
            # Insert new
            cur.execute("""
                INSERT INTO khazana_teachers (teacher_name, teacher_slug)
                VALUES (%s, %s)
            """, (teacher_name, teacher_slug))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row['id'] if row else None
    finally:
        conn.close()


def get_or_create_khazana_topic(program_name, topic_id, topic_name=None, subject_name=None, teacher_name=None):
    """
    Get or create a Khazana topic (e.g., "Graph Theory" within "Data Structure")
    Links program, subject, and teacher
    Returns: topic database ID
    """
    if not (program_name and topic_id):
        return None
    
    # Get/create foreign keys
    program_db_id = get_or_create_khazana_program(program_name)
    if not program_db_id:
        return None
    
    subject_db_id = get_or_create_khazana_subject(subject_name) if subject_name else None
    teacher_db_id = get_or_create_khazana_teacher(teacher_name) if teacher_name else None
    
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Try to get existing
            cur.execute("""
                SELECT id FROM khazana_topics
                WHERE program_id=%s AND topic_id=%s
            """, (program_db_id, topic_id))
            row = cur.fetchone()
            if row:
                # Update subject/teacher if provided
                if subject_db_id or teacher_db_id:
                    cur.execute("""
                        UPDATE khazana_topics
                        SET subject_id=COALESCE(%s, subject_id),
                            teacher_id=COALESCE(%s, teacher_id),
                            topic_name=COALESCE(%s, topic_name)
                        WHERE id=%s
                    """, (subject_db_id, teacher_db_id, topic_name, row['id']))
                    conn.commit()
                return row['id']
            
            # Insert new
            cur.execute("""
                INSERT INTO khazana_topics (
                    program_id, subject_id, teacher_id, topic_id, topic_name
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (program_db_id, subject_db_id, teacher_db_id, topic_id, topic_name))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row['id'] if row else None
    finally:
        conn.close()


def upsert_khazana_lecture_v2(
    program_name,
    topic_id,
    lecture_id,
    topic_name=None,
    subject_name=None,
    teacher_name=None,
    sub_topic_name=None,
    lecture_name=None,
    lecture_url=None,
    thumbnail_blob=None,
    thumbnail_mime=None,
    thumbnail_url=None,
    thumbnail_size=None,
    ia_identifier=None,
    ia_url=None,
    status=None,
    server_id=None,
    file_path=None,
    file_size=None,
    upload_bytes=None,
    upload_total=None,
    upload_percent=None,
    telegram_chat_id=None,
    telegram_message_id=None,
    telegram_file_id=None,
    error=None,
):
    """
    Upsert Khazana lecture using normalized schema
    Automatically creates program, subject, teacher, and topic if needed
    """
    if not (program_name and topic_id and lecture_id):
        return None
    
    # Get/create topic (which cascades to program, subject, teacher)
    topic_db_id = get_or_create_khazana_topic(
        program_name=program_name,
        topic_id=topic_id,
        topic_name=topic_name,
        subject_name=subject_name,
        teacher_name=teacher_name
    )
    if not topic_db_id:
        return None
    
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO khazana_contents (
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
                    error_text
                )
                VALUES (
                    %s, 'lecture', %s, %s, '', %s, %s, %s, %s, %s,
                    IF(%s IS NOT NULL OR %s IS NOT NULL, CURRENT_TIMESTAMP, NULL),
                    %s, %s, COALESCE(%s, 'pending'), %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    content_name=COALESCE(VALUES(content_name), content_name),
                    source_url=COALESCE(VALUES(source_url), source_url),
                    sub_topic_name=COALESCE(VALUES(sub_topic_name), sub_topic_name),
                    thumbnail_url=COALESCE(VALUES(thumbnail_url), thumbnail_url),
                    thumbnail_mime=COALESCE(VALUES(thumbnail_mime), thumbnail_mime),
                    thumbnail_size=COALESCE(VALUES(thumbnail_size), thumbnail_size),
                    thumbnail_blob=COALESCE(VALUES(thumbnail_blob), thumbnail_blob),
                    thumbnail_updated_at=IF(VALUES(thumbnail_blob) IS NOT NULL, CURRENT_TIMESTAMP, thumbnail_updated_at),
                    ia_identifier=COALESCE(VALUES(ia_identifier), ia_identifier),
                    ia_url=COALESCE(VALUES(ia_url), ia_url),
                    status=COALESCE(VALUES(status), status),
                    server_id=COALESCE(VALUES(server_id), server_id),
                    file_path=COALESCE(VALUES(file_path), file_path),
                    file_size=COALESCE(VALUES(file_size), file_size),
                    upload_bytes=COALESCE(VALUES(upload_bytes), upload_bytes),
                    upload_total=COALESCE(VALUES(upload_total), upload_total),
                    upload_percent=COALESCE(VALUES(upload_percent), upload_percent),
                    telegram_chat_id=COALESCE(VALUES(telegram_chat_id), telegram_chat_id),
                    telegram_message_id=COALESCE(VALUES(telegram_message_id), telegram_message_id),
                    telegram_file_id=COALESCE(VALUES(telegram_file_id), telegram_file_id),
                    error_text=COALESCE(VALUES(error_text), error_text)
            """, (
                topic_db_id,
                lecture_id,
                lecture_name,
                lecture_url,
                sub_topic_name,
                thumbnail_url,
                thumbnail_mime,
                thumbnail_size,
                thumbnail_blob,
                thumbnail_blob,
                thumbnail_url,
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
                error,
            ))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = cur.fetchone()
            return row['id'] if row else None
    finally:
        conn.close()


def get_khazana_lecture_status_v2(program_name, topic_id, lecture_id):
    """
    Get Khazana lecture status from normalized schema
    Returns: dict with status, file_path, ia_identifier, subject_name, teacher_name, etc.
    """
    if not (program_name and topic_id and lecture_id):
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                                        c.id,
                                        c.status,
                                        c.file_path,
                                        c.file_size,
                                        c.ia_identifier,
                                        c.ia_url,
                                        c.content_name AS lecture_name,
                                        c.source_url AS lecture_url,
                                        c.sub_topic_name,
                    t.topic_name,
                    s.subject_name,
                    teach.teacher_name,
                    p.program_name,
                                        c.created_at,
                                        c.updated_at
                                FROM khazana_contents c
                                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                LEFT JOIN khazana_subjects s ON t.subject_id = s.id
                LEFT JOIN khazana_teachers teach ON t.teacher_id = teach.id
                                WHERE c.content_type='lecture'
                                    AND p.program_name=%s
                                    AND t.topic_id=%s
                                    AND c.content_id=%s
            """, (program_name, topic_id, lecture_id))
            return cur.fetchone()
    finally:
        conn.close()


def list_khazana_lectures_v2(
    program_name=None,
    status=None,
    subject_name=None,
    teacher_name=None,
    topic_id=None,
    limit=1000,
):
    """
    List Khazana lectures from normalized schema in deterministic sequence
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            clauses = []
            params = []
            
            if program_name:
                clauses.append("p.program_name=%s")
                params.append(program_name)
            if status:
                clauses.append("c.status=%s")
                params.append(status)
            if subject_name:
                clean_subj = _clean_subject_name(subject_name)
                if clean_subj:
                    clauses.append("s.subject_name=%s")
                    params.append(clean_subj)
            if teacher_name:
                clauses.append("teach.teacher_name=%s")
                params.append(teacher_name)
            if topic_id:
                clauses.append("t.topic_id=%s")
                params.append(topic_id)
            
            safe_limit = max(1, min(int(limit or 1000), 10000))
            
            cur.execute(f"""
                SELECT
                    c.id,
                    p.program_name,
                    s.subject_name,
                    teach.teacher_name,
                    t.topic_id,
                    t.topic_name,
                    c.content_id AS lecture_id,
                    c.content_name AS lecture_name,
                    c.source_url AS lecture_url,
                    c.sub_topic_name,
                    c.status,
                    c.file_path,
                    c.file_size,
                    c.ia_identifier,
                    c.ia_url,
                    c.created_at,
                    c.updated_at
                FROM khazana_contents c
                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                LEFT JOIN khazana_subjects s ON t.subject_id = s.id
                LEFT JOIN khazana_teachers teach ON t.teacher_id = teach.id
                WHERE c.content_type='lecture'
                {('AND ' + ' AND '.join(clauses)) if clauses else ''}
                ORDER BY c.created_at ASC, c.id ASC
                LIMIT {safe_limit}
            """, params)
            return cur.fetchall()
    finally:
        conn.close()


def has_khazana_thumbnail_v2(program_name, topic_id, lecture_id):
    """Check if Khazana lecture has thumbnail in normalized schema"""
    if not (program_name and topic_id and lecture_id):
        return False
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                                FROM khazana_contents c
                                JOIN khazana_topics t ON c.topic_id = t.id
                JOIN khazana_programs p ON t.program_id = p.id
                                WHERE c.content_type='lecture'
                                    AND p.program_name=%s
                                    AND t.topic_id=%s
                                    AND c.content_id=%s
                                    AND c.thumbnail_blob IS NOT NULL
            """, (program_name, topic_id, lecture_id))
            return bool(cur.fetchone())
    finally:
        conn.close()

# ============================================================================
# Test Series Schema Functions
# ============================================================================


def upsert_test(
    batch_id,
    test_id,
    test_name=None,
    test_type=None,
    test_template=None,
    language_code=None,
    sections_json=None,
    difficulty_levels_json=None,
    source_url=None,
    status=None,
    error=None,
):
    """Upsert test record with batch_id + test_id as compound key"""
    if not (batch_id and test_id):
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tests (
                    batch_id,
                    test_id,
                    test_name,
                    test_type,
                    test_template,
                    language_code,
                    sections_json,
                    difficulty_levels_json,
                    source_url,
                    status,
                    error_text
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, 'pending'), %s
                )
                ON DUPLICATE KEY UPDATE
                    test_name=COALESCE(VALUES(test_name), test_name),
                    test_type=COALESCE(VALUES(test_type), test_type),
                    test_template=COALESCE(VALUES(test_template), test_template),
                    language_code=COALESCE(VALUES(language_code), language_code),
                    sections_json=COALESCE(VALUES(sections_json), sections_json),
                    difficulty_levels_json=COALESCE(VALUES(difficulty_levels_json), difficulty_levels_json),
                    source_url=COALESCE(VALUES(source_url), source_url),
                    status=COALESCE(VALUES(status), status),
                    error_text=COALESCE(VALUES(error_text), error_text)
                """,
                (
                    batch_id,
                    test_id,
                    test_name,
                    test_type,
                    test_template,
                    language_code,
                    sections_json,
                    difficulty_levels_json,
                    source_url,
                    status,
                    error,
                ),
            )
    finally:
        conn.close()


def get_test(batch_id, test_id):
    """Check if test exists; returns status for skipping if already processed"""
    if not (batch_id and test_id):
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, status, error_text
                FROM tests
                WHERE batch_id=%s AND test_id=%s
                """,
                (batch_id, test_id),
            )
            return cur.fetchone()
    finally:
        conn.close()


def upsert_test_question(
    batch_id,
    test_id,
    question_id,
    question_number=None,
    question_type=None,
    positive_marks=None,
    negative_marks=None,
    difficulty_level=None,
    section_id=None,
    subject_id=None,
    chapter_id=None,
    topic_id=None,
    sub_topic_id=None,
    qbg_id=None,
    qbg_subject_id=None,
    qbg_chapter_id=None,
    qbg_topic_id=None,
    correct_option_ids_json=None,
    correct_answer_text=None,
):
    """Upsert question record with batch_id + test_id + question_id as compound key"""
    if not (batch_id and test_id and question_id):
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO test_questions (
                    batch_id,
                    test_id,
                    question_id,
                    question_number,
                    question_type,
                    positive_marks,
                    negative_marks,
                    difficulty_level,
                    section_id,
                    subject_id,
                    chapter_id,
                    topic_id,
                    sub_topic_id,
                    qbg_id,
                    qbg_subject_id,
                    qbg_chapter_id,
                    qbg_topic_id,
                    correct_option_ids_json,
                    correct_answer_text
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    question_number=COALESCE(VALUES(question_number), question_number),
                    question_type=COALESCE(VALUES(question_type), question_type),
                    positive_marks=COALESCE(VALUES(positive_marks), positive_marks),
                    negative_marks=COALESCE(VALUES(negative_marks), negative_marks),
                    difficulty_level=COALESCE(VALUES(difficulty_level), difficulty_level),
                    section_id=COALESCE(VALUES(section_id), section_id),
                    subject_id=COALESCE(VALUES(subject_id), subject_id),
                    chapter_id=COALESCE(VALUES(chapter_id), chapter_id),
                    topic_id=COALESCE(VALUES(topic_id), topic_id),
                    sub_topic_id=COALESCE(VALUES(sub_topic_id), sub_topic_id),
                    qbg_id=COALESCE(VALUES(qbg_id), qbg_id),
                    qbg_subject_id=COALESCE(VALUES(qbg_subject_id), qbg_subject_id),
                    qbg_chapter_id=COALESCE(VALUES(qbg_chapter_id), qbg_chapter_id),
                    qbg_topic_id=COALESCE(VALUES(qbg_topic_id), qbg_topic_id),
                    correct_option_ids_json=COALESCE(VALUES(correct_option_ids_json), correct_option_ids_json),
                    correct_answer_text=COALESCE(VALUES(correct_answer_text), correct_answer_text)
                """,
                (
                    batch_id,
                    test_id,
                    question_id,
                    question_number,
                    question_type,
                    positive_marks,
                    negative_marks,
                    difficulty_level,
                    section_id,
                    subject_id,
                    chapter_id,
                    topic_id,
                    sub_topic_id,
                    qbg_id,
                    qbg_subject_id,
                    qbg_chapter_id,
                    qbg_topic_id,
                    correct_option_ids_json,
                    correct_answer_text,
                ),
            )
    finally:
        conn.close()


def upsert_test_option(batch_id, test_id, question_id, option_id, option_text=None):
    """Upsert option with batch_id + test_id + question_id + option_id as compound key"""
    if not (batch_id and test_id and question_id and option_id):
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO test_options (
                    batch_id,
                    test_id,
                    question_id,
                    option_id,
                    option_text
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    option_text=COALESCE(VALUES(option_text), option_text)
                """,
                (batch_id, test_id, question_id, option_id, option_text),
            )
    finally:
        conn.close()


def upsert_test_asset(
    batch_id,
    test_id,
    question_id,
    asset_kind,
    source_key,
    source_url=None,
    asset_type=None,
    file_path=None,
    file_size=None,
    file_mime=None,
    storage_provider=None,
    storage_id=None,
    storage_url=None,
    ia_identifier=None,
    ia_url=None,
    youtube_id=None,
    status=None,
    error=None,
):
    """Upsert test asset (image/video) with compound key: batch_id + test_id + question_id + asset_kind + source_key"""
    if not (batch_id and test_id and question_id and asset_kind and source_key):
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO test_assets (
                    batch_id,
                    test_id,
                    question_id,
                    asset_kind,
                    source_key,
                    source_url,
                    asset_type,
                    file_path,
                    file_size,
                    file_mime,
                    storage_provider,
                    storage_id,
                    storage_url,
                    ia_identifier,
                    ia_url,
                    youtube_id,
                    status,
                    error_text
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, 'pending'), %s
                )
                ON DUPLICATE KEY UPDATE
                    source_url=COALESCE(VALUES(source_url), source_url),
                    asset_type=COALESCE(VALUES(asset_type), asset_type),
                    file_path=COALESCE(VALUES(file_path), file_path),
                    file_size=COALESCE(VALUES(file_size), file_size),
                    file_mime=COALESCE(VALUES(file_mime), file_mime),
                    storage_provider=COALESCE(VALUES(storage_provider), storage_provider),
                    storage_id=COALESCE(VALUES(storage_id), storage_id),
                    storage_url=COALESCE(VALUES(storage_url), storage_url),
                    ia_identifier=COALESCE(VALUES(ia_identifier), ia_identifier),
                    ia_url=COALESCE(VALUES(ia_url), ia_url),
                    youtube_id=COALESCE(VALUES(youtube_id), youtube_id),
                    status=COALESCE(VALUES(status), status),
                    error_text=COALESCE(VALUES(error_text), error_text)
                """,
                (
                    batch_id,
                    test_id,
                    question_id,
                    asset_kind,
                    source_key,
                    source_url,
                    asset_type,
                    file_path,
                    file_size,
                    file_mime,
                    storage_provider,
                    storage_id,
                    storage_url,
                    ia_identifier,
                    ia_url,
                    youtube_id,
                    status,
                    error,
                ),
            )
    finally:
        conn.close()


def get_test_asset_by_source(batch_id, test_id, asset_kind, source_key):
    """Get asset record by source_key for deduplication check"""
    if not (batch_id and test_id and asset_kind and source_key):
        return None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, storage_provider, storage_id, storage_url, ia_identifier, ia_url, youtube_id
                FROM test_assets
                WHERE batch_id=%s AND test_id=%s AND asset_kind=%s AND source_key=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (batch_id, test_id, asset_kind, source_key),
            )
            return cur.fetchone()
    finally:
        conn.close()
