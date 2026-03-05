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

-- Khazana Lectures (video lectures)
CREATE TABLE IF NOT EXISTS khazana_lectures (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    topic_id BIGINT NOT NULL,
    lecture_id VARCHAR(128) NOT NULL,
    lecture_name VARCHAR(255) NULL,
    lecture_url TEXT NULL,
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
    UNIQUE KEY uniq_khazana_lecture (topic_id, lecture_id),
    KEY idx_khazana_lecture_ia (ia_identifier),
    KEY idx_khazana_lecture_status (status),
    CONSTRAINT fk_khazana_lecture_topic FOREIGN KEY (topic_id)
        REFERENCES khazana_topics(id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Khazana Assets (DPPs, notes, etc.)
CREATE TABLE IF NOT EXISTS khazana_assets (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    topic_id BIGINT NOT NULL,
    content_id VARCHAR(128) NOT NULL,
    content_name VARCHAR(255) NULL,
    kind VARCHAR(64) NOT NULL,
    file_url TEXT NULL,
    file_path TEXT NULL,
    file_size BIGINT NULL,
    ia_identifier VARCHAR(255) NULL,
    ia_url TEXT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    server_id VARCHAR(128) NULL,
    error_text TEXT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_khazana_asset (topic_id, content_id, kind),
    KEY idx_khazana_asset_ia (ia_identifier),
    KEY idx_khazana_asset_status (status),
    CONSTRAINT fk_khazana_asset_topic FOREIGN KEY (topic_id)
        REFERENCES khazana_topics(id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Legacy table for backward compatibility (to be migrated)
CREATE TABLE IF NOT EXISTS khazana_lecture_uploads_old (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    program_name VARCHAR(128) NOT NULL,
    subject_name VARCHAR(128) NULL,
    teacher_name VARCHAR(255) NULL,
    topic_name VARCHAR(255) NULL,
    sub_topic_name VARCHAR(255) NULL,
    lecture_id VARCHAR(128) NOT NULL,
    lecture_name VARCHAR(255) NULL,
    lecture_url TEXT NULL,
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
    UNIQUE KEY uniq_khazana_lecture (program_name, lecture_id, topic_name)
) ENGINE=InnoDB;

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
