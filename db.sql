CREATE TABLE IF NOT EXISTS courses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(128) NOT NULL,
    batch_slug VARCHAR(128) NULL,
    name VARCHAR(255) NULL,
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
