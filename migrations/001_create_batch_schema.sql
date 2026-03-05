-- Migration: create batch download schema
-- Source: db_batch_downloads.sql

-- Create batches and related tables required by batch_dl_v2.py

CREATE TABLE IF NOT EXISTS batches (
    id VARCHAR(50) PRIMARY KEY,
    slug VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

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
    UNIQUE KEY `uq_batch_subject` (batch_id, slug)
);

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
    UNIQUE KEY `uq_batch_chapter` (batch_id, subject_id, slug)
);

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
);

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
);

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
);

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
);

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
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_batch_created ON batches(created_at);
CREATE INDEX IF NOT EXISTS idx_subject_batch ON batch_subjects(batch_id);
CREATE INDEX IF NOT EXISTS idx_chapter_subject ON batch_chapters(subject_id);
CREATE INDEX IF NOT EXISTS idx_notes_batch ON notes(batch_id);
CREATE INDEX IF NOT EXISTS idx_notes_chapter ON notes(chapter_id);
CREATE INDEX IF NOT EXISTS idx_dpp_batch ON dpp_notes(batch_id);
CREATE INDEX IF NOT EXISTS idx_attachment_type ON note_attachments(attachment_type);
CREATE INDEX IF NOT EXISTS idx_dpp_attachment_type ON dpp_attachments(attachment_type);

-- View: Complete attachment list with batch info
CREATE OR REPLACE VIEW v_all_attachments AS
SELECT 
    'note' as attachment_type,
    n.id as attachment_id,
    n.note_id as content_id,
    na.file_name,
    na.file_url,
    na.file_path_local,
    na.file_size_bytes,
    bs.name as subject_name,
    bc.name as chapter_name,
    b.name as batch_name,
    b.slug as batch_slug,
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
    'dpp' as attachment_type,
    da.id as attachment_id,
    da.dpp_id as content_id,
    da.file_name,
    da.file_url,
    da.file_path_local,
    da.file_size_bytes,
    bs.name as subject_name,
    bc.name as chapter_name,
    b.name as batch_name,
    b.slug as batch_slug,
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
