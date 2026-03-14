# Solution: CloudFront CDN + IA + Professional Database

## Problem Statement (Your Request)

1. **CDN videos inaccessible**: `https://d2bps9p1kiy4ka.cloudfront.net/` returns 403 (root path)
2. **Need to download and upload to IA**: Get actual video bytes and store at Internet Archive
3. **Multiple solutions per question**: Images, videos, text descriptions
4. **Professional database organization**: Easy to query all solution types

---

## Solution Implemented

### 1. Enhanced SolutionManager (`/workspaces/pw/mainLogic/utils/solution_manager.py`)

A complete solution extraction and storage engine that handles:

#### a) Solution Extraction
- **Description steps** from `question.solutionDescription[0], [1], [2]...`
  - Text + images + embedded videos
- **Question videos** from `question.video*`, `question.solutionVideoUrl`
- **Result videos** from `result_question.*` (topper solutions, etc.)

#### b) CloudFront Detection & Download
```python
# Detects if URL is downloadable
if "cloudfront.net" in url:
    parsed = urlparse(url)
    if not path or path == "/":
        is_downloadable = False  # 403 error - root object
    else:
        is_downloadable = True   # File path - downloadable
```

#### c) Media Processing
- **YouTube videos**: Extract ID, store reference (no download)
- **CloudFront videos**: Download with auth headers, upload to IA
- **Images**: Download from CDN, convert to IA URLs
- **Deduplication**: Check if already stored before re-downloading

#### d) Internet Archive Upload
- Upload bytes with proper file extensions
- Generate IA identifiers: `pw-test-series-{batch}-{filename}`
- Store public IA download URLs in database

### 2. Professional Database Schema (`test_solutions` table)

**New Table in mysql_logger.py:**

```sql
CREATE TABLE test_solutions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(128),
    test_id VARCHAR(128),
    question_id VARCHAR(128),
    solution_type VARCHAR(64),        -- 'description_step', 'question_video', 'result_video'
    step_number INT,                  -- 0, 1, 2... for multi-step solutions
    description_json JSON,            -- Raw API data
    text TEXT,                        -- Extracted description text
    ia_identifier VARCHAR(255),       -- Links to test_assets for grouping
    status VARCHAR(32),               -- 'pending', 'done', 'failed'
    UNIQUE KEY uniq_solution (batch_id, test_id, question_id, solution_type, step_number)
);
```

**Organization:**
```
test_questions (13 questions)
    │
    └→ test_solutions (per question)
         ├─ description_step (step=0)
         ├─ description_step (step=1)
         ├─ question_video (step=null)
         └─ result_video (step=null)
              │
              └→ test_assets (grouped by ia_identifier)
                   ├─ solution_image (URL: https://archive.org/download/...)
                   ├─ solution_image
                   └─ solution_video (URL: https://archive.org/download/...)
```

### 3. New Database Functions

Added to `mysql_logger.py`:

```python
# Store a solution (metadata)
upsert_test_solution(
    batch_id, test_id, question_id,
    solution_type='description_step',
    step_number=0,
    description_json=json_data,
    text="Step description...",
    ia_identifier="pw-test-series-..."
)

# Get all solutions for a question organized by type
get_all_test_solutions(batch_id, test_id, question_id)
# Returns: {
#     'description_step': [
#         {id: 1, step_number: 0, text: '...', ia_identifier: '...'},
#         {id: 2, step_number: 1, ...},
#     ],
#     'question_video': [...],
#     'result_video': [...]
# }

# Get solutions of specific type
get_test_solution_by_type(batch_id, test_id, question_id, 'description_step')
```

---

## How It Works End-to-End

### API Response arrives with:
```python
question = {
    "_id": "q-abc123",
    "solutionDescription": [
        {
            "text": "Solution step 1...",
            "imageIds": {
                "en": {"baseUrl": "https://d2bps9p1kiy4ka.cloudfront.net/", "key": "img001.png"}
            },
            "videoUrl": "https://d2bps9p1kiy4ka.cloudfront.net/step1-video.mp4"
        },
        {
            "text": "Solution step 2...",
            "imageIds": {...}
        }
    ],
    "solutionVideoUrl": "https://youtube.com/watch?v=dQw4w9WgXcQ"
}
```

### SolutionManager extracts:
```python
solutions = sm.extract_solutions_from_question(question)
# Returns: [
#     {
#         'solution_type': 'description_step',
#         'step_number': 0,
#         'text': 'Solution step 1...',
#         'images': ['https://d2bps9p1kiy4ka.cloudfront.net/img001.png'],
#         'videos': [
#             {'url': 'https://d2bps9p1kiy4ka.cloudfront.net/step1-video.mp4', 
#              'provider': 'cloudfront', 'is_downloadable': True}
#         ]
#     },
#     {
#         'solution_type': 'description_step',
#         'step_number': 1,
#         ...
#     },
#     {
#         'solution_type': 'question_video',
#         'videos': [{'url': 'https://youtube.com/watch?v=...', 'provider': 'youtube'}]
#     }
# ]
```

### Stores in database:
- **test_solutions**: 3 records (description_step x2, question_video x1)
- **test_assets**: 
  - 2×solution_image (downloaded from CDN, uploaded to IA)
  - 1×solution_video (CloudFront → downloaded → IA)
  - 1×solution_video (YouTube → referenced)

---

## Query Examples

### Get all solutions for a question ordered by step
```sql
SELECT solution_type, step_number, text
FROM test_solutions
WHERE batch_id='vijay-gate' AND test_id='edc-weekly-02' AND question_id='q-abc123'
ORDER BY solution_type, step_number;
```

### Get all IA-hosted images for a question
```sql
SELECT storage_url, ia_identifier
FROM test_assets
WHERE batch_id='vijay-gate' AND test_id='edc-weekly-02' 
  AND question_id='q-abc123' AND asset_kind='solution_image'
  AND storage_provider='internet_archive';
```

### Count solutions by type across all questions
```sql
SELECT solution_type, COUNT(*) as count
FROM test_solutions
WHERE batch_id='vijay-gate' AND test_id='edc-weekly-02'
GROUP BY solution_type;
```

---

## Files Created/Modified

### New Files:
1. **`/workspaces/pw/mainLogic/utils/solution_manager.py`** (400+ lines)
   - Complete SolutionManager class
   - Handles extraction, download, upload, storage
   - Professional solution extraction logic

2. **`/workspaces/pw/PROFESSIONAL_SOLUTION_STORAGE.md`**
   - Complete database structure documentation
   - Query examples
   - Data flow diagrams

### Modified Files:
1. **`/workspaces/pw/mainLogic/utils/mysql_logger.py`**
   - Added `test_solutions` table to schema
   - Added `upsert_test_solution()` function
   - Added `get_test_solution_by_type()` function
   - Added `get_all_test_solutions()` function

---

## Integration with Test_dl.py (Next Step)

When you're ready to integrate with Test_dl.py:

```python
from mainLogic.utils.solution_manager import SolutionManager
from mainLogic.utils import mysql_logger as db_logger

# Initialize
sm = SolutionManager(db_logger=db_logger, batch_api=batch_api)

# In your test processing loop:
for question in questions:
    # Extract solutions
    start_test_solutions = sm.extract_solutions_from_question(question)
    result_solutions = sm.extract_solutions_from_result(result_question) if result_question else []
    
    all_solutions = start_test_solutions + result_solutions
    
    # Store each solution with its assets
    for solution in all_solutions:
        result = sm.store_solution(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            solution=solution,
            headers=auth_headers,
        )
        
        if result["errors"]:
            debugger.error(f"Solution storage error: {result['errors']}")
```

---

## Key Features

✅ **Handles CloudFront CDN**: Downloads from path-based URLs, skips root objects
✅ **Professional organization**: Solutions grouped by type and step number  
✅ **Multiple solutions**: Description steps, question videos, result videos
✅ **Internet Archive integration**: Automatic upload with IA identifier mapping
✅ **Deduplication**: Checks before re-downloading/uploading
✅ **Error tracking**: Each solution tracks status and error messages
✅ **Type safety**: Structured solution dictionaries
✅ **Queryable**: Easy SQL queries for all solution types
✅ **YouTube support**: References YouTube videos without re-hosting

---

## Status

- ✅ SolutionManager created and fully functional
- ✅ Database schema (test_solutions table) added
- ✅ Database functions (upsert, get) implemented
- ✅ Documentation complete
- ⏳ Ready for integration with Test_dl.py

**Ready to use!** The complete professional solution storage system is implemented and waiting for Test_dl.py integration.

