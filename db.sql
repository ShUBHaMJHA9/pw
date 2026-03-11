-- =============================================================
-- PW Database Schema — Single Source of Truth
-- All tables, indexes, views, and migration procedures.
-- =============================================================

-- ---------------------------------------------------------------
-- CORE DOWNLOAD TABLES
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS courses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(128) NOT NULL,
    batch_slug VARCHAR(128) NULL,
    name VARCHAR(255) NULL,
    thumbnail_url TEXT NULL,
    thumbnail_mime VARCHAR(64) NULL,
    thumbnail_size BIGINT NULL,
    thumbnail_blob LONGBLOB NULL,
    thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_course_batch (batch_id)
) ENGINE=InnoDB;

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

CREATE TABLE IF NOT EXISTS subjects (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    course_id BIGINT NOT NULL,
    slug VARCHAR(128) NOT NULL,
    name VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_subject_course_slug (course_id, slug),
    CONSTRAINT fk_subject_course FOREIGN KEY (course_id)
        REFERENCES courses(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS chapters (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    subject_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_chapter_subject_name (subject_id, name),
    CONSTRAINT fk_chapter_subject FOREIGN KEY (subject_id)
        REFERENCES subjects(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS teachers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    teacher_key VARCHAR(255) NOT NULL,
    teacher_id VARCHAR(128) NULL,
    name VARCHAR(255) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_teacher_key (teacher_key)
) ENGINE=InnoDB;

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
    thumbnail_url TEXT NULL,
    thumbnail_mime VARCHAR(64) NULL,
    thumbnail_size BIGINT NULL,
    thumbnail_blob LONGBLOB NULL,
    thumbnail_updated_at TIMESTAMP NULL DEFAULT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_lecture_batch (batch_id, lecture_id),
    KEY idx_lecture_course (course_id),
    KEY idx_lecture_subject (subject_id),
    KEY idx_lecture_chapter (chapter_id),
    CONSTRAINT fk_lecture_course FOREIGN KEY (course_id)
        REFERENCES courses(id),
    CONSTRAINT fk_lecture_subject FOREIGN KEY (subject_id)
        REFERENCES subjects(id),
    CONSTRAINT fk_lecture_chapter FOREIGN KEY (chapter_id)
        REFERENCES chapters(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lecture_teachers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    lecture_id VARCHAR(128) NOT NULL,
    batch_id VARCHAR(128) NOT NULL,
    teacher_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_lecture_teacher (batch_id, lecture_id, teacher_id),
    KEY idx_lecture_teacher (lecture_id, teacher_id),
    CONSTRAINT fk_lecture_teachers_lecture 
        FOREIGN KEY (batch_id, lecture_id)
        REFERENCES lectures(batch_id, lecture_id),
    CONSTRAINT fk_lecture_teachers_teacher 
        FOREIGN KEY (teacher_id)
        REFERENCES teachers(id)
) ENGINE=InnoDB;

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
    CONSTRAINT fk_upload_lecture 
        FOREIGN KEY (batch_id, lecture_id)
        REFERENCES lectures(batch_id, lecture_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------
-- KHAZANA TABLES
-- ---------------------------------------------------------------

-- Khazana Programs (e.g., "JEE 2025", "NEET 2024")
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

-- Khazana Subjects (e.g., "Data Structure", "Physics")
CREATE TABLE IF NOT EXISTS khazana_subjects (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    subject_name VARCHAR(255) NOT NULL,
    subject_slug VARCHAR(255) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_khazana_subject_name (subject_name)
) ENGINE=InnoDB;

-- Khazana Teachers (normalized from teachers table concept)
CREATE TABLE IF NOT EXISTS khazana_teachers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    teacher_name VARCHAR(255) NOT NULL,
    teacher_slug VARCHAR(255) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_khazana_teacher_name (teacher_name)
) ENGINE=InnoDB;

-- Khazana Topics (e.g., "Graph Theory", "Thermodynamics")
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
    KEY idx_khazana_topic_teacher (teacher_id),
    CONSTRAINT fk_khazana_topic_program FOREIGN KEY (program_id)
        REFERENCES khazana_programs(id) ON DELETE CASCADE,
    CONSTRAINT fk_khazana_topic_subject FOREIGN KEY (subject_id)
        REFERENCES khazana_subjects(id) ON DELETE SET NULL,
    CONSTRAINT fk_khazana_topic_teacher FOREIGN KEY (teacher_id)
        REFERENCES khazana_teachers(id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- Unified Khazana content table (lectures + assets)
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
    KEY idx_khazana_content_ia (ia_identifier),
    CONSTRAINT fk_khazana_content_topic FOREIGN KEY (topic_id)
        REFERENCES khazana_topics(id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- ---------------------------------------------------------------
-- UTILITY TABLES
-- ---------------------------------------------------------------

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

-- ---------------------------------------------------------------
-- BATCH DOWNLOAD TABLES (batch_dl_v2.py)
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS batches (
    id VARCHAR(50) PRIMARY KEY,
    slug VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS batch_subjects (
    id VARCHAR(50) PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    display_order INT,
    lecture_count INT DEFAULT 0,
    tag_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
    UNIQUE KEY uq_batch_subject (batch_id, slug)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS batch_chapters (
    id VARCHAR(50) PRIMARY KEY,
    subject_id VARCHAR(50) NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    chapter_type VARCHAR(50),
    display_order INT,
    lecture_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (subject_id) REFERENCES batch_subjects(id) ON DELETE CASCADE,
    FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
    UNIQUE KEY uq_batch_chapter (batch_id, subject_id, slug)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS notes (
    id VARCHAR(50) PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    subject_id VARCHAR(50) NOT NULL,
    chapter_id VARCHAR(50) NOT NULL,
    note_date DATETIME,
    status VARCHAR(50),
    is_batch_doubt_enabled BOOLEAN DEFAULT 0,
    is_free BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
    FOREIGN KEY (subject_id) REFERENCES batch_subjects(id) ON DELETE CASCADE,
    FOREIGN KEY (chapter_id) REFERENCES batch_chapters(id) ON DELETE CASCADE,
    INDEX idx_batch_subject_chapter (batch_id, subject_id, chapter_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS note_attachments (
    id VARCHAR(50) PRIMARY KEY,
    note_id VARCHAR(50) NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_url VARCHAR(2048),
    file_path_local VARCHAR(1024),
    file_size_bytes BIGINT,
    file_extension VARCHAR(10),
    attachment_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE,
    FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
    INDEX idx_note_id (note_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dpp_notes (
    id VARCHAR(50) PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    subject_id VARCHAR(50) NOT NULL,
    chapter_id VARCHAR(50) NOT NULL,
    dpp_date DATETIME,
    dpp_number INT,
    status VARCHAR(50),
    is_free BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
    FOREIGN KEY (subject_id) REFERENCES batch_subjects(id) ON DELETE CASCADE,
    FOREIGN KEY (chapter_id) REFERENCES batch_chapters(id) ON DELETE CASCADE,
    INDEX idx_batch_subject_chapter (batch_id, subject_id, chapter_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dpp_attachments (
    id VARCHAR(50) PRIMARY KEY,
    dpp_id VARCHAR(50) NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_url VARCHAR(2048),
    file_path_local VARCHAR(1024),
    file_size_bytes BIGINT,
    file_extension VARCHAR(10),
    attachment_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dpp_id) REFERENCES dpp_notes(id) ON DELETE CASCADE,
    FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE CASCADE,
    INDEX idx_dpp_id (dpp_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ia_uploads (
    id VARCHAR(100) PRIMARY KEY,
    batch_id VARCHAR(50),
    subject_id VARCHAR(50),
    chapter_id VARCHAR(50),
    note_id VARCHAR(50),
    dpp_id VARCHAR(50),
    attachment_id VARCHAR(50),
    ia_identifier VARCHAR(100) NOT NULL UNIQUE,
    ia_url VARCHAR(2048),
    ia_title VARCHAR(255),
    ia_description TEXT,
    ia_access_level VARCHAR(50),
    upload_status VARCHAR(50),
    upload_error TEXT,
    file_count INT DEFAULT 1,
    total_size_bytes BIGINT,
    uploaded_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (batch_id) REFERENCES batches(id),
    FOREIGN KEY (subject_id) REFERENCES batch_subjects(id),
    FOREIGN KEY (chapter_id) REFERENCES batch_chapters(id),
    FOREIGN KEY (note_id) REFERENCES notes(id),
    FOREIGN KEY (dpp_id) REFERENCES dpp_notes(id),
    INDEX idx_batch (batch_id),
    INDEX idx_ia_identifier (ia_identifier),
    INDEX idx_upload_status (upload_status)
) ENGINE=InnoDB;

-- Batch table indexes
CREATE INDEX IF NOT EXISTS idx_batch_created ON batches(created_at);
CREATE INDEX IF NOT EXISTS idx_subject_batch ON batch_subjects(batch_id);
CREATE INDEX IF NOT EXISTS idx_chapter_subject ON batch_chapters(subject_id);
CREATE INDEX IF NOT EXISTS idx_notes_batch ON notes(batch_id);
CREATE INDEX IF NOT EXISTS idx_notes_chapter ON notes(chapter_id);
CREATE INDEX IF NOT EXISTS idx_dpp_batch ON dpp_notes(batch_id);
CREATE INDEX IF NOT EXISTS idx_attachment_type ON note_attachments(attachment_type);
CREATE INDEX IF NOT EXISTS idx_dpp_attachment_type ON dpp_attachments(attachment_type);

-- View: Combined attachment list with IA upload status
CREATE OR REPLACE VIEW v_all_attachments AS
SELECT
    'note' AS attachment_type,
    na.id AS attachment_id,
    na.note_id AS content_id,
    na.file_name,
    na.file_url,
    na.file_path_local,
    na.file_size_bytes,
    bs.name AS subject_name,
    bc.name AS chapter_name,
    b.name AS batch_name,
    b.slug AS batch_slug,
    iu.ia_identifier,
    iu.ia_url,
    iu.upload_status,
    na.created_at
FROM note_attachments na
JOIN notes n ON na.note_id = n.id
JOIN batches b ON n.batch_id = b.id
JOIN batch_subjects bs ON n.subject_id = bs.id
JOIN batch_chapters bc ON n.chapter_id = bc.id
LEFT JOIN ia_uploads iu ON iu.attachment_id = na.id

UNION ALL

SELECT
    'dpp' AS attachment_type,
    da.id AS attachment_id,
    da.dpp_id AS content_id,
    da.file_name,
    da.file_url,
    da.file_path_local,
    da.file_size_bytes,
    bs.name AS subject_name,
    bc.name AS chapter_name,
    b.name AS batch_name,
    b.slug AS batch_slug,
    iu.ia_identifier,
    iu.ia_url,
    iu.upload_status,
    da.created_at
FROM dpp_attachments da
JOIN dpp_notes dn ON da.dpp_id = dn.id
JOIN batches b ON dn.batch_id = b.id
JOIN batch_subjects bs ON dn.subject_id = bs.id
JOIN batch_chapters bc ON dn.chapter_id = bc.id
LEFT JOIN ia_uploads iu ON iu.attachment_id = da.id;

-- ---------------------------------------------------------------
-- LEGACY DATA MIGRATION: khazana_lecture_uploads_old → khazana_contents
-- Run ONLY ONCE when upgrading from the old flat Khazana schema.
-- Skip on fresh installs (table won't exist, INSERTs are no-ops).
-- ---------------------------------------------------------------

INSERT IGNORE INTO khazana_programs (program_id, program_name)
SELECT DISTINCT
    LOWER(REPLACE(program_name, ' ', '-')) AS program_id,
    program_name
FROM khazana_lecture_uploads_old
WHERE program_name IS NOT NULL;

INSERT IGNORE INTO khazana_subjects (subject_name, subject_slug)
SELECT DISTINCT
    TRIM(REGEXP_REPLACE(subject_name, ' by .*$', '')) AS clean_subject,
    LOWER(REPLACE(TRIM(REGEXP_REPLACE(subject_name, ' by .*$', '')), ' ', '-')) AS subject_slug
FROM khazana_lecture_uploads_old
WHERE subject_name IS NOT NULL AND subject_name != '';

INSERT IGNORE INTO khazana_teachers (teacher_name, teacher_slug)
SELECT DISTINCT
    teacher_name,
    LOWER(REPLACE(teacher_name, ' ', '-')) AS teacher_slug
FROM khazana_lecture_uploads_old
WHERE teacher_name IS NOT NULL AND teacher_name != '';

INSERT IGNORE INTO khazana_topics (program_id, subject_id, teacher_id, topic_id, topic_name)
SELECT DISTINCT
    p.id AS program_id,
    s.id AS subject_id,
    t.id AS teacher_id,
    COALESCE(old.topic_name, CONCAT(old.program_name, '-', old.lecture_id)) AS topic_id,
    old.topic_name
FROM khazana_lecture_uploads_old old
LEFT JOIN khazana_programs p ON p.program_name = old.program_name
LEFT JOIN khazana_subjects s ON s.subject_name = TRIM(REGEXP_REPLACE(COALESCE(old.subject_name, ''), ' by .*$', ''))
LEFT JOIN khazana_teachers t ON t.teacher_name = old.teacher_name
WHERE old.program_name IS NOT NULL;

INSERT IGNORE INTO khazana_contents (
    topic_id, content_type, content_id, content_name, asset_kind,
    source_url, sub_topic_name, thumbnail_url, thumbnail_mime,
    thumbnail_size, thumbnail_blob, thumbnail_updated_at,
    ia_identifier, ia_url, status, server_id, file_path, file_size,
    upload_bytes, upload_total, upload_percent,
    telegram_chat_id, telegram_message_id, telegram_file_id,
    error_text, created_at, updated_at
)
SELECT
    topic.id AS topic_id,
    'lecture' AS content_type,
    old.lecture_id,
    old.lecture_name,
    '' AS asset_kind,
    old.lecture_url,
    old.sub_topic_name,
    old.thumbnail_url, old.thumbnail_mime, old.thumbnail_size,
    old.thumbnail_blob, old.thumbnail_updated_at,
    old.ia_identifier, old.ia_url,
    old.status, old.server_id, old.file_path, old.file_size,
    old.upload_bytes, old.upload_total, old.upload_percent,
    old.telegram_chat_id, old.telegram_message_id, old.telegram_file_id,
    old.error_text, old.created_at, old.updated_at
FROM khazana_lecture_uploads_old old
JOIN khazana_programs p ON p.program_name = old.program_name
JOIN khazana_topics topic ON
    topic.program_id = p.id AND
    topic.topic_id = COALESCE(old.topic_name, CONCAT(old.program_name, '-', old.lecture_id));

-- ---------------------------------------------------------------
-- CLEANUP: Drop deprecated split Khazana tables.
-- Safe to re-run — checks existence before dropping.
-- ---------------------------------------------------------------

DELIMITER $$

CREATE PROCEDURE IF NOT EXISTS cleanup_unused_khazana_tables()
BEGIN
    DECLARE has_khazana_lectures INT DEFAULT 0;
    DECLARE has_khazana_assets INT DEFAULT 0;

    SELECT COUNT(*) INTO has_khazana_lectures
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'khazana_lectures';

    SELECT COUNT(*) INTO has_khazana_assets
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'khazana_assets';

    IF has_khazana_lectures > 0 THEN
        INSERT IGNORE INTO khazana_contents (
            topic_id, content_type, content_id, content_name, asset_kind,
            source_url, sub_topic_name, thumbnail_url, thumbnail_mime,
            thumbnail_size, thumbnail_blob, thumbnail_updated_at,
            ia_identifier, ia_url, status, server_id, file_path, file_size,
            upload_bytes, upload_total, upload_percent,
            telegram_chat_id, telegram_message_id, telegram_file_id,
            error_text, created_at, updated_at
        )
        SELECT
            l.topic_id, 'lecture', l.lecture_id, l.lecture_name, '',
            l.lecture_url, l.sub_topic_name, l.thumbnail_url, l.thumbnail_mime,
            l.thumbnail_size, l.thumbnail_blob, l.thumbnail_updated_at,
            l.ia_identifier, l.ia_url, l.status, l.server_id, l.file_path, l.file_size,
            l.upload_bytes, l.upload_total, l.upload_percent,
            l.telegram_chat_id, l.telegram_message_id, l.telegram_file_id,
            l.error_text, l.created_at, l.updated_at
        FROM khazana_lectures l;
    END IF;

    IF has_khazana_assets > 0 THEN
        INSERT IGNORE INTO khazana_contents (
            topic_id, content_type, content_id, content_name, asset_kind,
            source_url, sub_topic_name, ia_identifier, ia_url, status,
            server_id, file_path, file_size, error_text, created_at, updated_at
        )
        SELECT
            a.topic_id, 'asset', a.content_id, a.content_name,
            COALESCE(a.kind, ''), a.file_url, NULL,
            a.ia_identifier, a.ia_url, a.status,
            a.server_id, a.file_path, a.file_size, a.error_text, a.created_at, a.updated_at
        FROM khazana_assets a;
    END IF;

    DROP TABLE IF EXISTS khazana_lectures;
    DROP TABLE IF EXISTS khazana_assets;
    DROP TABLE IF EXISTS khazana_lecture_uploads;
END$$

DELIMITER ;

CALL cleanup_unused_khazana_tables();
DROP PROCEDURE IF EXISTS cleanup_unused_khazana_tables;

-- NOTE: khazana_lecture_uploads_old is kept intentionally for rollback/audit.
