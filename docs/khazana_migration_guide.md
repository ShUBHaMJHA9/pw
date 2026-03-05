# Khazana Database Normalization - Migration Guide

## Overview

The Khazana database schema has been refactored from a flat denormalized structure to a properly normalized relational schema. This provides:

- **Clean subject names** (e.g., "Data Structure" instead of "Data Structure by Teacher Name")
- **Proper separation** of programs, subjects, teachers, topics, and lectures
- **Better data integrity** with foreign key constraints
- **Batch/Program thumbnails** fully supported
- **Easier queries** for subject-wise or teacher-wise filtering

## New Schema Structure

### Tables Created

1. **khazana_programs** - Stores program info (e.g., "JEE 2025", "NEET 2024")
   - `program_id` (slug, unique)
   - `program_name` (display name)
   - Thumbnail columns (blob + metadata)

2. **khazana_subjects** - Clean subject names (e.g., "Physics", "Data Structure")
   - `subject_name` (unique, cleaned of "by Teacher" suffix)
   - `subject_slug`

3. **khazana_teachers** - Teacher information
   - `teacher_name` (unique)
   - `teacher_slug`

4. **khazana_topics** - Topics linking program + subject + teacher
   - `topic_id` (unique within program)
   - `topic_name` (display name)
   - Foreign keys to program, subject, teacher

5. **khazana_lectures** - Video lectures
   - Links to topic (which gives access to program/subject/teacher)
   - All previous columns (status, IA identifier, file paths, etc.)
   - `UNIQUE KEY (topic_id, lecture_id)`

6. **khazana_assets** - DPPs, notes, etc. (updated)
   - Links to topic instead of flat program_name
   - Same content tracking as before

### Legacy Tables

- `khazana_lecture_uploads_old` - Backup of old flat schema (created during migration)

## Migration Steps

### 1. Apply New Schema

```bash
# Connect to MySQL and run the updated db.sql
mysql -u your_user -p your_database < db.sql
```

This will:
- Create all new normalized tables
- Rename old `khazana_lecture_uploads` to `khazana_lecture_uploads_old` for backup

### 2. Run Migration Script

```bash
# Python migration tool (recommended)
python3 tools/migrate_khazana.py
```

OR

```bash
# SQL migration script
mysql -u your_user -p your_database < migrations/002_migrate_khazana_normalized.sql
```

The migration will:
1. Extract unique programs, subjects, teachers from old data
2. Clean subject names (remove "by Teacher" suffix)
3. Create slug identifiers
4. Populate normalized tables
5. Link all lectures to their topics

### 3. Verify Migration

```bash
# Check counts
mysql -u your_user -p -e "
SELECT 
    (SELECT COUNT(*) FROM khazana_programs) as programs,
    (SELECT COUNT(*) FROM khazana_subjects) as subjects,
    (SELECT COUNT(*) FROM khazana_teachers) as teachers,
    (SELECT COUNT(*) FROM khazana_topics) as topics,
    (SELECT COUNT(*) FROM khazana_lectures) as lectures,
    (SELECT COUNT(*) FROM khazana_lecture_uploads_old) as old_lectures
" your_database
```

All new table counts should be > 0 and lectures count should match old_lectures count.

## Code Changes

### khazana_dl.py

- Updated to use `*_v2()` functions from mysql_logger
- Automatically creates normalized records (program → subject → teacher → topic)
- All existing functionality preserved (download, decrypt, merge, IA upload, TUI progress)

### mysql_logger.py

New functions added (backward compatible):

#### Get/Create Functions
- `get_or_create_khazana_program(program_name, thumbnail_url, ...)` - Auto-insert programs
- `get_or_create_khazana_subject(subject_name)` - Auto-insert subjects (cleans names)
- `get_or_create_khazana_teacher(teacher_name)` - Auto-insert teachers
- `get_or_create_khazana_topic(program_name, topic_id, topic_name, subject_name, teacher_name)` - Links all together

#### Lecture Functions
- `upsert_khazana_lecture_v2()` - Replaces `upsert_khazana_lecture()` with normalized schema
- `get_khazana_lecture_status_v2()` - Returns lecture status + joined subject/teacher names
- `list_khazana_lectures_v2()` - Query lectures with subject/teacher filters
- `has_khazana_thumbnail_v2()` - Check thumbnail existence

#### Migrated Functions
- Old functions (`upsert_khazana_lecture`, etc.) still in code but point to old table
- Can be fully removed after verifying migration success

## Usage Examples

### Download Lectures (No Code Changes Required!)

```bash
# Khazana downloader works exactly as before
python3 khazana_dl.py
```

The code now automatically:
1. Creates program record with thumbnail if available
2. Creates subject record (clean name, no "by Teacher" suffix)
3. Creates teacher record if teacher info available
4. Links topic to program + subject + teacher
5. Stores lecture with all metadata

### Query Lectures by Subject

```python
from mainLogic.utils import mysql_logger

# Get all "Physics" lectures across all programs
lectures = mysql_logger.list_khazana_lectures_v2(
    subject_name="Physics",
    status="done",
    limit=100
)

for lec in lectures:
    print(f"{lec['program_name']} - {lec['subject_name']} - {lec['lecture_name']}")
```

### Query Lectures by Teacher

```python
lectures = mysql_logger.list_khazana_lectures_v2(
    teacher_name="John Doe",
    limit=50
)
```

### Get Clean Subject Names

```sql
-- All unique subjects (clean names, no teacher suffix)
SELECT subject_name, subject_slug, COUNT(*) as topic_count
FROM khazana_subjects s
JOIN khazana_topics t ON s.id = t.subject_id
GROUP BY s.id
ORDER BY subject_name;
```

### Get Topics by Subject and Teacher

```sql
-- All "Data Structure" topics taught by specific teacher
SELECT 
    p.program_name,
    s.subject_name,
    teach.teacher_name,
    t.topic_name,
    COUNT(l.id) as lecture_count
FROM khazana_topics t
JOIN khazana_programs p ON t.program_id = p.id
LEFT JOIN khazana_subjects s ON t.subject_id = s.id
LEFT JOIN khazana_teachers teach ON t.teacher_id = teach.id
LEFT JOIN khazana_lectures l ON l.topic_id = t.id
WHERE s.subject_name = 'Data Structure'
  AND teach.teacher_name = 'Alice Smith'
GROUP BY t.id;
```

## Rollback Plan

If you need to rollback:

1. **Restore old table**:
```sql
DROP TABLE khazana_lectures;
DROP TABLE khazana_assets;
DROP TABLE khazana_topics;
DROP TABLE khazana_teachers;
DROP TABLE khazana_subjects;
DROP TABLE khazana_programs;

RENAME TABLE khazana_lecture_uploads_old TO khazana_lecture_uploads;
```

2. **Revert code** (git):
```bash
git checkout HEAD~1 -- khazana_dl.py mainLogic/utils/mysql_logger.py
```

## Benefits

### Before (Flat Schema)
```
khazana_lecture_uploads:
- program_name: "JEE 2025"
- subject_name: "Data Structure by Alice Smith"
- teacher_name: "Alice Smith"
- topic_name: "Graph Theory"
- lecture_id: "12345"
- (repeated for every lecture)
```

**Problems**: Duplicated data, inconsistent naming, hard to query by subject

### After (Normalized Schema)
```
khazana_programs: JEE 2025 (id=1)
khazana_subjects: Data Structure (id=5, clean name!)
khazana_teachers: Alice Smith (id=10)
khazana_topics: Graph Theory (id=20, links to program 1 + subject 5 + teacher 10)
khazana_lectures: Lecture 12345 (links to topic 20)
```

**Benefits**: 
- Clean subject names automatically
- No duplication
- Easy subject-wise queries
- Proper foreign key integrity
- Supports batch/program thumbnails

## Support

- Migration tool: `tools/migrate_khazana.py`
- SQL migration: `migrations/002_migrate_khazana_normalized.sql`
- Schema: `db.sql` (updated)
- API: `mainLogic/utils/mysql_logger.py` (backward compatible)

All old code continues to work during transition period!
