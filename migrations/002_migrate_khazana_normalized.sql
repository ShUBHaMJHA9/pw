-- Migration script to transform old khazana_lecture_uploads to normalized schema
-- Run this after creating the new tables

-- Step 1: Backup existing data to old table
-- RENAME TABLE khazana_lecture_uploads TO khazana_lecture_uploads_old;
-- (Already done in db.sql schema update)

-- Step 2: Populate khazana_programs from distinct program names
INSERT IGNORE INTO khazana_programs (program_id, program_name)
SELECT DISTINCT 
    LOWER(REPLACE(program_name, ' ', '-')) as program_id,
    program_name
FROM khazana_lecture_uploads_old
WHERE program_name IS NOT NULL;

-- Step 3: Populate khazana_subjects from distinct clean subject names
INSERT IGNORE INTO khazana_subjects (subject_name, subject_slug)
SELECT DISTINCT
    -- Extract clean subject name (remove "by Teacher" suffix if present)
    TRIM(REGEXP_REPLACE(subject_name, ' by .*$', '')) as clean_subject,
    LOWER(REPLACE(TRIM(REGEXP_REPLACE(subject_name, ' by .*$', '')), ' ', '-')) as subject_slug
FROM khazana_lecture_uploads_old
WHERE subject_name IS NOT NULL AND subject_name != '';

-- Step 4: Populate khazana_teachers from distinct teacher names
INSERT IGNORE INTO khazana_teachers (teacher_name, teacher_slug)
SELECT DISTINCT
    teacher_name,
    LOWER(REPLACE(teacher_name, ' ', '-')) as teacher_slug
FROM khazana_lecture_uploads_old
WHERE teacher_name IS NOT NULL AND teacher_name != '';

-- Step 5: Populate khazana_topics with program, subject, and teacher relationships
INSERT IGNORE INTO khazana_topics (
    program_id,
    subject_id,
    teacher_id,
    topic_id,
    topic_name
)
SELECT DISTINCT
    p.id as program_id,
    s.id as subject_id,
    t.id as teacher_id,
    COALESCE(old.topic_name, CONCAT(old.program_name, '-', old.lecture_id)) as topic_id,
    old.topic_name
FROM khazana_lecture_uploads_old old
LEFT JOIN khazana_programs p ON p.program_name = old.program_name
LEFT JOIN khazana_subjects s ON s.subject_name = TRIM(REGEXP_REPLACE(COALESCE(old.subject_name, ''), ' by .*$', ''))
LEFT JOIN khazana_teachers t ON t.teacher_name = old.teacher_name
WHERE old.program_name IS NOT NULL;

-- Step 6: Migrate lecture data to khazana_lectures
INSERT IGNORE INTO khazana_lectures (
    topic_id,
    lecture_id,
    lecture_name,
    lecture_url,
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
SELECT
    topic.id as topic_id,
    old.lecture_id,
    old.lecture_name,
    old.lecture_url,
    old.sub_topic_name,
    old.thumbnail_url,
    old.thumbnail_mime,
    old.thumbnail_size,
    old.thumbnail_blob,
    old.thumbnail_updated_at,
    old.ia_identifier,
    old.ia_url,
    old.status,
    old.server_id,
    old.file_path,
    old.file_size,
    old.upload_bytes,
    old.upload_total,
    old.upload_percent,
    old.telegram_chat_id,
    old.telegram_message_id,
    old.telegram_file_id,
    old.error_text,
    old.created_at,
    old.updated_at
FROM khazana_lecture_uploads_old old
JOIN khazana_programs p ON p.program_name = old.program_name
JOIN khazana_topics topic ON 
    topic.program_id = p.id AND
    topic.topic_id = COALESCE(old.topic_name, CONCAT(old.program_name, '-', old.lecture_id));

-- Step 7: Populate khazana_assets_old as backup (if exists)
CREATE TABLE IF NOT EXISTS khazana_assets_old LIKE khazana_assets;

-- Note: You may need to manually migrate khazana_assets if they exist
-- The migration will follow similar pattern as lectures
