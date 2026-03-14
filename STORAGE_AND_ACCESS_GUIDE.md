# Test Series Storage & Access Guide

## Overview

When you download a test series using `Test_dl.py`, data is stored in **two places**:

1. **Internet Archive (IA)** — Images and videos from questions/solutions
2. **MySQL Database** — Metadata, question details, marks, and storage references

---

## 1. INTERNET ARCHIVE (IA) — What Gets Uploaded

### Upload Logic
- **Source**: Question images from API (`imageIds`) and solution videos from descriptions
- **Destination**: Internet Archive at `https://archive.org/details/{identifier}`
- **Identifier Format**: `pw-test-series-{batch-name}-{question-source-key}`
- **Files Stored**: 
  - PNG/JPG images (question images)
  - PNG/JPG images (solution step images)
  - MP4 videos (if not YouTube reference)

### What Triggers IA Upload
```python
# In Test_dl.py:

# 1. Question images (extracted from imageIds)
_store_question_image(
    db_logger, batch_id, test_id, question_id,
    source_key="q-{image-id}",
    image_url="https://pw-cdn...",
    headers=auth_headers
)

# 2. Solution images (from solutionDescription array)
_store_solution_image(
    db_logger, batch_id, test_id, question_id,
    source_key="sol-image-{step}",
    image_url="https://...",
    headers=auth_headers
)

# 3. Solution videos (from solutionDescription array)
# → YouTube videos stored as reference (no upload)
# → Non-YouTube videos uploaded to IA
_store_solution_video(
    db_logger, batch_id, test_id, question_id,
    source_key="sol-video-{step}",
    video_url="https://...",
    headers=auth_headers
)
```

### Deduplication
- Before uploading, checks if asset already exists via `source_key`
- If exists with `storage_url`, reuses that URL (no re-upload)
- Prevents duplicate uploads of same image/video

---

## 2. DATABASE TABLES — What Gets Stored

### Table: `tests`
**Purpose**: Test metadata (high-level)

```sql
-- Key columns:
batch_id      VARCHAR(128)  -- Course/batch identifier
test_id       VARCHAR(128)  -- Unique test ID
test_name     VARCHAR(255)  -- Test display name
test_type     VARCHAR(64)   -- e.g., "weekly_test", "full_test"
language_code VARCHAR(32)   -- e.g., "en", "hi"
sections_json JSON          -- All sections metadata
source_url    TEXT          -- API endpoint URL
ia_identifier VARCHAR(255)  -- Internet Archive collection ID (if test thumbnail uploaded)
status        VARCHAR(32)   -- 'pending', 'done', 'failed'
error_text    TEXT          -- Error message if failed
```

### Table: `test_questions`
**Purpose**: Individual questions with marks and metadata

```sql
-- Key columns:
batch_id         VARCHAR(128)
test_id          VARCHAR(128)
question_id      VARCHAR(128)     -- Unique question ID
question_number  INT              -- Position in test (1, 2, 3...)
question_type    VARCHAR(64)      -- "single", "multiple", "subjective"
positive_marks   DECIMAL(10,4)    -- ⭐ Marks for correct answer
negative_marks   DECIMAL(10,4)    -- ⭐ Negative marking for wrong
difficulty_level INT              -- 1-5
section_id       VARCHAR(128)     -- Which section in test
subject_id       VARCHAR(128)     -- Subject (if available)
chapter_id       VARCHAR(128)     -- Chapter (if available)
topic_id         VARCHAR(128)     -- Topic (if available)
correct_option_ids_json JSON       -- Array of correct option IDs
correct_answer_text TEXT           -- For subjective questions
```

### Table: `test_options`
**Purpose**: Answer options for each question

```sql
-- Key columns:
batch_id        VARCHAR(128)
test_id         VARCHAR(128)
question_id     VARCHAR(128)     -- Parent question
option_id       VARCHAR(128)     -- Unique option identifier
option_text     TEXT             -- Option content (may include HTML/image refs)
```

### Table: `test_assets`
**Purpose**: All images and videos (questions + solutions)

```sql
-- Key columns:
batch_id         VARCHAR(128)
test_id          VARCHAR(128)
question_id      VARCHAR(128)     -- Which question (or NULL for test-level assets)
asset_kind       VARCHAR(64)      -- 'question_image', 'solution_image', 'solution_video'
source_key       VARCHAR(255)     -- Deduplication key: "q-{id}", "sol-image-{index}"
source_url       TEXT             -- Original API URL
storage_provider VARCHAR(64)      -- 'internet_archive' or 'youtube'
storage_id       VARCHAR(255)     -- IA identifier or YouTube ID
storage_url      TEXT             -- Download/view URL
ia_identifier    VARCHAR(255)     -- IA collection identifier
youtube_id       VARCHAR(64)      -- YouTube video ID (if provider=youtube)
ia_url           TEXT             -- Full IA details page URL
status           VARCHAR(32)      -- 'pending', 'done', 'failed'
error_text       TEXT             -- Error if upload/processing failed
```

---

## 3. HOW TO ACCESS YOUR DATA

### Query 1: Get All Questions from a Test

```sql
SELECT 
    question_number,
    question_id,
    question_type,
    positive_marks,
    negative_marks,
    difficulty_level,
    subject_id,
    topic_id
FROM test_questions
WHERE batch_id = 'vijay-gate'  -- Your batch
  AND test_id = 'edc-weekly-02' -- Your test
ORDER BY question_number;
```

**Result**: 
- Question #1: +2 marks, -0.5 marks, Single type, Topic: BJT
- Question #2: +2 marks, -0.5 marks, Multiple type, Topic: Diodes
- ... etc

---

### Query 2: Get All Images for a Question

```sql
SELECT 
    asset_kind,        -- 'question_image' or 'solution_image'
    source_url,        -- Original URL from PW API
    storage_url,       -- Downloaded URL at IA
    ia_identifier      -- IA collection name
FROM test_assets
WHERE batch_id = 'vijay-gate'
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc'
  AND asset_kind IN ('question_image', 'solution_image')
ORDER BY id;
```

**Result**: 
- Question image: `https://archive.org/download/pw-test-series-...`
- Solution step 1: `https://archive.org/download/pw-test-series-...`
- Solution step 2: `https://archive.org/download/pw-test-series-...`

---

### Query 3: Get Solution Videos for a Question

```sql
SELECT 
    asset_kind,        -- Always 'solution_video'
    storage_provider,  -- 'youtube' or 'internet_archive'
    youtube_id,        -- YouTube video ID
    storage_url        -- Full video URL
FROM test_assets
WHERE batch_id = 'vijay-gate'
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc'
  AND asset_kind = 'solution_video'
ORDER BY id;
```

**Result**:
- `https://www.youtube.com/watch?v=dQw4w9WgXcQ` (YouTube)
- `https://archive.org/download/pw-test-series-...` (IA hosted)

---

### Query 4: Get All Answer Options for a Question

```sql
SELECT 
    option_id,
    option_text
FROM test_options
WHERE batch_id = 'vijay-gate'
  AND test_id = 'edc-weekly-02'
  AND question_id = 'q-61a23f1bc'
ORDER BY option_id;
```

**Result**:
- Option A: "The field effect transistor..."
- Option B: "The bipolar junction transistor..."
- Option C: "Both A and B..."
- Option D: "None of the above..."

---

### Query 5: Check Upload Status & Errors

```sql
SELECT 
    asset_kind,
    COUNT(*) as count,
    SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as uploaded,
    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
FROM test_assets
WHERE batch_id = 'vijay-gate'
  AND test_id = 'edc-weekly-02'
GROUP BY asset_kind;
```

**Result**:
```
question_image    | 13 | 13 | 0   ✓ All uploaded
solution_image    | 26 | 26 | 0   ✓ All uploaded
solution_video    | 13 | 12 | 1   ⚠ One video failed
```

---

### Query 6: Find Failed Uploads (Errors)

```sql
SELECT 
    question_id,
    asset_kind,
    source_key,
    source_url,
    error_text
FROM test_assets
WHERE batch_id = 'vijay-gate'
  AND test_id = 'edc-weekly-02'
  AND status = 'failed'
ORDER BY question_id, asset_kind;
```

**Result**: Shows exactly which uploads failed and why

---

### Query 7: Get All Test Metadata (Sections, Languages)

```sql
SELECT 
    test_id,
    test_name,
    test_type,
    language_code,
    sections_json,
    status,
    created_at
FROM tests
WHERE batch_id = 'vijay-gate'
ORDER BY created_at DESC;
```

**Result**: Full test metadata including JSON arrays of sections

---

## 4. QUICK PYTHON SCRIPT — Access Data Programmatically

```python
import pymysql
import json

# Connect
conn = pymysql.connect(
    host="16.170.205.250",
    user="temp",
    password="Shubhamjha2005",
    database="temp"
)

def get_test_summary(batch_id, test_id):
    """Get complete test data structure"""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        # Get test metadata
        cur.execute(
            "SELECT * FROM tests WHERE batch_id=%s AND test_id=%s",
            (batch_id, test_id)
        )
        test = cur.fetchone()
        
        # Get all questions with marks
        cur.execute(
            """SELECT question_id, question_number, question_type, 
                      positive_marks, negative_marks, topic_id, subject_id
               FROM test_questions 
               WHERE batch_id=%s AND test_id=%s
               ORDER BY question_number""",
            (batch_id, test_id)
        )
        questions = cur.fetchall()
        
        return {
            "test": test,
            "questions": questions,
            "question_count": len(questions),
            "total_marks": sum(q["positive_marks"] or 0 for q in questions)
        }

def get_question_assets(batch_id, test_id, question_id):
    """Get all assets (images + videos) for a question"""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """SELECT asset_kind, storage_provider, storage_url, youtube_id
               FROM test_assets
               WHERE batch_id=%s AND test_id=%s AND question_id=%s
               AND status='done'
               ORDER BY asset_kind""",
            (batch_id, test_id, question_id)
        )
        return cur.fetchall()

# Usage
summary = get_test_summary("vijay-gate", "edc-weekly-02")
print(f"Test: {summary['test']['test_name']}")
print(f"Questions: {summary['question_count']}")
print(f"Total Marks: {summary['total_marks']}")

for q in summary['questions'][:3]:  # First 3 questions
    assets = get_question_assets("vijay-gate", "edc-weekly-02", q['question_id'])
    print(f"\nQ{q['question_number']}: +{q['positive_marks']}/-{q['negative_marks']}")
    for asset in assets:
        print(f"  - {asset['asset_kind']}: {asset['storage_url']}")
```

---

## 5. INTERNET ARCHIVE ACCESS

### Direct IA Collection URLs
Your uploaded files are accessible at:
```
https://archive.org/details/pw-test-series-{identifier}
https://archive.org/download/pw-test-series-{identifier}/
```

### Example: Download Question Image Directly
```bash
# If storage_url is:
# https://archive.org/download/pw-test-series-edc-weekly-02-q1/image.png

curl "https://archive.org/download/pw-test-series-edc-weekly-02-q1/image.png" \
     -o question_1.png
```

### Batch Download All Assets from Test
```bash
# Download all images for a test from IA
ia download pw-test-series-edc-weekly-02 --dry-run

# Or use the storage_url directly in a loop
```

---

## 6. SUMMARY: Data Flow

```
API (PW System)
    ↓
Question data (imageIds, solutionDescription, etc.)
    ↓
Test_dl.py processes:
    ├─ Extracts question metadata → test_questions table
    ├─ Extracts options → test_options table
    ├─ Uploads images → Internet Archive
    └─ Uploads videos → Internet Archive OR links YouTube
    ↓
Database stores:
    ├─ tests: Test-level metadata
    ├─ test_questions: Questions with marks
    ├─ test_options: Answer choices
    ├─ test_assets: References to uploaded images/videos (IA URLs)
    ↓
Access via:
    ├─ SQL queries (get marks, topics, options)
    ├─ Python script (fetch complete test structure)
    └─ Direct IA URLs (download images/videos)
```

---

## 7. KEY TAKEAWAYS

✅ **Images & Videos uploaded to Internet Archive** — Accessible via HTTP URLs  
✅ **Question metadata in MySQL** — Marks, question IDs, topics, sections  
✅ **All data linked together** — test_assets table has both source URL and storage URL  
✅ **Deduplication prevents re-uploads** — Same image reused if already uploaded  
✅ **YouTube videos referenced, not re-uploaded** — Linked directly to YouTube  

**Everything is queryable and accessible!**
