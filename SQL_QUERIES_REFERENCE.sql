-- ============================================================
-- TEST SERIES DATA: QUICK SQL QUERIES
-- Copy-paste these queries to explore your downloaded tests
-- ============================================================

-- ============================================================
-- 1. OVERVIEW & SUMMARY QUERIES
-- ============================================================

-- List all batches with test counts
SELECT batch_id, COUNT(*) as test_count, 
       MAX(created_at) as latest_test
FROM tests
GROUP BY batch_id
ORDER BY batch_id;


-- List all tests in a batch
SELECT test_id, test_name, test_type, 
       (SELECT COUNT(*) FROM test_questions tq 
        WHERE tq.batch_id=t.batch_id AND tq.test_id=t.test_id) as questions,
       status, created_at
FROM tests t
WHERE batch_id = 'vijay-gate'  -- CHANGE THIS
ORDER BY created_at DESC;


-- Get test summary: metadata and counts
SELECT 
    t.test_name,
    t.test_type,
    t.language_code,
    COUNT(DISTINCT tq.question_id) as question_count,
    SUM(tq.positive_marks) as total_marks,
    COUNT(DISTINCT ta.id) as asset_count
FROM tests t
LEFT JOIN test_questions tq USING (batch_id, test_id)
LEFT JOIN test_assets ta USING (batch_id, test_id)
WHERE t.batch_id = 'vijay-gate' AND t.test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY t.batch_id, t.test_id;


-- ============================================================
-- 2. QUESTIONS & MARKS
-- ============================================================

-- View all questions with marks (easier to read)
SELECT 
    question_number as Q#,
    SUBSTRING(question_id, 1, 8) as QID,
    question_type as Type,
    positive_marks as `+Marks`,
    negative_marks as `-Marks`,
    difficulty_level as Diff,
    SUBSTRING(topic_id, 1, 20) as Topic
FROM test_questions
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
ORDER BY question_number;


-- Calculate total marks and average question marks
SELECT 
    COUNT(*) as total_questions,
    SUM(positive_marks) as total_marks,
    AVG(positive_marks) as avg_marks_per_question,
    MAX(positive_marks) as max_marks,
    MIN(positive_marks) as min_marks
FROM test_questions
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02';  -- CHANGE THESE


-- Get questions grouped by difficulty level
SELECT 
    difficulty_level,
    COUNT(*) as count,
    SUM(positive_marks) as total_marks
FROM test_questions
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY difficulty_level
ORDER BY difficulty_level DESC;


-- Get questions grouped by topic
SELECT 
    topic_id,
    COUNT(*) as question_count,
    SUM(positive_marks) as total_marks
FROM test_questions
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY topic_id
ORDER BY total_marks DESC;


-- ============================================================
-- 3. OPTIONS AND ANSWERS
-- ============================================================

-- Get all options for a specific question
SELECT 
    option_id,
    SUBSTRING(option_text, 1, 80) as option_preview,
    LENGTH(option_text) as text_length
FROM test_options
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc'  -- CHANGE THIS
ORDER BY option_id;


-- Check correct answer for a question
SELECT 
    question_number,
    SUBSTRING(question_id, 1, 8) as question_id,
    correct_option_ids_json as correct_options,
    correct_answer_text
FROM test_questions
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc';  -- CHANGE THIS


-- Count options per question (should be 4 for MCQ)
SELECT 
    question_number,
    COUNT(DISTINCT option_id) as option_count
FROM test_options
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY question_id
ORDER BY question_number;


-- ============================================================
-- 4. ASSETS: IMAGES AND VIDEOS
-- ============================================================

-- Get asset summary by type
SELECT 
    asset_kind,
    COUNT(*) as total_assets,
    SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as uploaded,
    SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
FROM test_assets
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY asset_kind;


-- Get all images for a question
SELECT 
    asset_kind,
    storage_provider,
    storage_url,
    ia_identifier,
    status
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc'
  AND asset_kind IN ('question_image', 'solution_image')  -- CHANGE QUESTION_ID
ORDER BY id;


-- Get solution videos for a question
SELECT 
    storage_provider,
    youtube_id,
    storage_url,
    ia_identifier,
    status
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc'  -- CHANGE THIS
  AND asset_kind = 'solution_video';


-- Get all IA-hosted assets (direct Internet Archive downloads)
SELECT 
    question_id,
    asset_kind,
    SUBSTRING(ia_identifier, 1, 40) as ia_id,
    storage_url
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'  -- CHANGE THESE
  AND storage_provider = 'internet_archive'
  AND status = 'done'
ORDER BY question_id, asset_kind;


-- Get YouTube solution videos
SELECT 
    question_id,
    youtube_id,
    CONCAT('https://www.youtube.com/watch?v=', youtube_id) as youtube_url
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'  -- CHANGE THESE
  AND storage_provider = 'youtube'
  AND status = 'done';


-- ============================================================
-- 5. ASSET UPLOAD STATUS
-- ============================================================

-- Find all failed uploads with error messages
SELECT 
    question_id,
    asset_kind,
    source_key,
    SUBSTRING(source_url, 1, 60) as source_url,
    SUBSTRING(error_text, 1, 100) as error
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'  -- CHANGE THESE
  AND status = 'failed'
ORDER BY question_id;


-- Find duplicate assets (same source_url uploaded multiple times)
SELECT 
    question_id,
    asset_kind,
    source_url,
    COUNT(*) as count
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY question_id, asset_kind, source_url
HAVING COUNT(*) > 1;


-- Show asset upload timeline (when each asset was uploaded)
SELECT 
    question_id,
    asset_kind,
    COUNT(*) as count,
    MIN(created_at) as first_upload,
    MAX(updated_at) as last_update
FROM test_assets
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY DATE(created_at), asset_kind
ORDER BY created_at DESC;


-- ============================================================
-- 6. COMPLEX QUERIES
-- ============================================================

-- Get complete question structure with options and assets
SELECT 
    tq.question_number,
    tq.question_id,
    tq.question_type,
    tq.positive_marks,
    tq.negative_marks,
    COUNT(DISTINCT to1.option_id) as option_count,
    COUNT(DISTINCT ta1.id) as asset_count
FROM test_questions tq
LEFT JOIN test_options to1 ON to1.batch_id=tq.batch_id 
                          AND to1.test_id=tq.test_id 
                          AND to1.question_id=tq.question_id
LEFT JOIN test_assets ta1 ON ta1.batch_id=tq.batch_id 
                         AND ta1.test_id=tq.test_id 
                         AND ta1.question_id=tq.question_id
WHERE tq.batch_id = 'vijay-gate' AND tq.test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY tq.question_id
ORDER BY tq.question_number;


-- Get test completeness score (how much data is available)
SELECT 
    t.test_name,
    COUNT(DISTINCT tq.question_id) as questions,
    COUNT(DISTINCT to1.option_id) as has_options,
    COUNT(DISTINCT CASE WHEN ta1.asset_kind='question_image' THEN ta1.id END) as question_images,
    COUNT(DISTINCT CASE WHEN ta1.asset_kind='solution_image' THEN ta1.id END) as solution_images,
    COUNT(DISTINCT CASE WHEN ta1.asset_kind='solution_video' THEN ta1.id END) as solution_videos,
    ROUND(100 * 
        COUNT(DISTINCT CASE WHEN to1.option_id IS NOT NULL THEN tq.question_id END) / 
        COUNT(DISTINCT tq.question_id)
    ) as options_coverage_pct
FROM tests t
LEFT JOIN test_questions tq USING (batch_id, test_id)
LEFT JOIN test_options to1 ON to1.batch_id=t.batch_id 
                          AND to1.test_id=t.test_id 
                          AND to1.question_id=tq.question_id
LEFT JOIN test_assets ta1 ON ta1.batch_id=t.batch_id 
                         AND ta1.test_id=t.test_id 
                         AND ta1.question_id=tq.question_id
WHERE t.batch_id = 'vijay-gate' AND t.test_id = 'edc-weekly-02'  -- CHANGE THESE
GROUP BY t.batch_id, t.test_id;


-- ============================================================
-- 7. DATA CLEANUP & MAINTENANCE
-- ============================================================

-- Find orphaned options (options without parent question)
SELECT 
    batch_id, test_id, question_id, COUNT(*) as orphan_count
FROM test_options
WHERE (batch_id, test_id, question_id) NOT IN (
    SELECT batch_id, test_id, question_id FROM test_questions
)
GROUP BY batch_id, test_id, question_id;


-- Find orphaned assets (assets without parent question)
SELECT 
    batch_id, test_id, question_id, asset_kind, COUNT(*) as orphan_count
FROM test_assets
WHERE question_id IS NOT NULL
  AND (batch_id, test_id, question_id) NOT IN (
    SELECT batch_id, test_id, question_id FROM test_questions
)
GROUP BY batch_id, test_id, question_id, asset_kind;


-- Delete failed assets (cautious — verify first!)
-- SELECT * FROM test_assets WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02' AND status = 'failed';
-- DELETE FROM test_assets WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02' AND status = 'failed';


-- ============================================================
-- 8. EXPORT QUERIES
-- ============================================================

-- Export all questions (comma-separated)
SELECT 
    question_number,
    question_id,
    question_type,
    positive_marks,
    negative_marks,
    difficulty_level,
    topic_id
FROM test_questions
WHERE batch_id = 'vijay-gate' AND test_id = 'edc-weekly-02'  -- CHANGE THESE
ORDER BY question_number
INTO OUTFILE '/tmp/test_questions.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n';


-- Export assets with direct download links
SELECT 
    question_id,
    asset_kind,
    storage_url as download_url,
    ia_identifier
FROM test_assets
WHERE batch_id = 'vijay-gate' 
  AND test_id = 'edc-weekly-02'  -- CHANGE THESE
  AND status = 'done'
ORDER BY question_id, asset_kind
INTO OUTFILE '/tmp/test_assets.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n';
