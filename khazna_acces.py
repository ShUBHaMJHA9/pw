#!/usr/bin/env python3
"""Khazana lecture access helper (database-only).

Purpose:
- Read Khazana lecture content from DB in proper sequence.
- Keep Khazana and normal course lecture flows separate for frontend AI.
- Selection order: Batch(program) -> Subject -> Teacher -> Lecture.

Output:
- Prints a final JSON block with selected lecture details.
"""

import json
import sys
from datetime import datetime

from mainLogic.utils.glv_var import debugger

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def _pick_one(items, title, label_getter):
    if not items:
        return None
    print(title)
    for idx, item in enumerate(items, start=1):
        print(f"  {idx}. {label_getter(item)}")
    choice = input("Choose one [1]: ").strip() or "1"
    try:
        i = int(choice) - 1
    except Exception:
        i = 0
    if i < 0 or i >= len(items):
        i = 0
    return items[i]


def _init_db():
    try:
        from mainLogic.utils import mysql_logger as db

        db.init(None)
        db.ensure_schema()
        return db
    except Exception as exc:
        debugger.error(f"DB init failed: {exc}")
        return None


def _list_batches_from_db(db):
    """List Khazana batches from DB only.

    In normalized schema, Khazana batch is represented by khazana_programs.
    """
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    p.program_id,
                    COALESCE(NULLIF(TRIM(p.program_name), ''), p.program_id) AS batch_name
                FROM khazana_programs p
                JOIN khazana_topics t ON t.program_id = p.id
                JOIN khazana_contents c ON c.topic_id = t.id
                WHERE c.content_type = 'lecture'
                ORDER BY batch_name ASC
                """
            )
            rows = cur.fetchall() or []
    finally:
        conn.close()

    return [
        {
            "batch_name": row["batch_name"],
            "batch_slug": None,
            "program_id": row["program_id"],
        }
        for row in rows
        if row.get("program_id")
    ]


def _get_subjects_for_program(db, program_id):
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    COALESCE(s.subject_name, 'Unknown Subject') AS subject_name
                FROM khazana_contents c
                JOIN khazana_topics t ON t.id = c.topic_id
                JOIN khazana_programs p ON p.id = t.program_id
                LEFT JOIN khazana_subjects s ON s.id = t.subject_id
                WHERE p.program_id = %s
                  AND c.content_type = 'lecture'
                ORDER BY subject_name ASC
                """,
                (program_id,),
            )
            return [row["subject_name"] for row in (cur.fetchall() or [])]
    finally:
        conn.close()


def _get_teachers_for_subject(db, program_id, subject_name):
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    COALESCE(k.teacher_name, 'Unknown Teacher') AS teacher_name
                FROM khazana_contents c
                JOIN khazana_topics t ON t.id = c.topic_id
                JOIN khazana_programs p ON p.id = t.program_id
                LEFT JOIN khazana_subjects s ON s.id = t.subject_id
                LEFT JOIN khazana_teachers k ON k.id = t.teacher_id
                WHERE p.program_id = %s
                  AND COALESCE(s.subject_name, 'Unknown Subject') = %s
                  AND c.content_type = 'lecture'
                ORDER BY teacher_name ASC
                """,
                (program_id, subject_name),
            )
            return [row["teacher_name"] for row in (cur.fetchall() or [])]
    finally:
        conn.close()


def _get_topics_for_teacher_subject(db, program_id, subject_name, teacher_name):
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    t.topic_id,
                    COALESCE(NULLIF(TRIM(t.topic_name), ''), t.topic_id) AS topic_name
                FROM khazana_contents c
                JOIN khazana_topics t ON t.id = c.topic_id
                JOIN khazana_programs p ON p.id = t.program_id
                LEFT JOIN khazana_subjects s ON s.id = t.subject_id
                LEFT JOIN khazana_teachers k ON k.id = t.teacher_id
                WHERE p.program_id = %s
                  AND COALESCE(s.subject_name, 'Unknown Subject') = %s
                  AND COALESCE(k.teacher_name, 'Unknown Teacher') = %s
                  AND c.content_type = 'lecture'
                ORDER BY topic_name ASC
                """,
                (program_id, subject_name, teacher_name),
            )
            rows = cur.fetchall() or []
            return [
                {
                    "topic_id": row["topic_id"],
                    "topic_name": row["topic_name"],
                }
                for row in rows
                if row.get("topic_id")
            ]
    finally:
        conn.close()


def _get_lectures_sequence(db, program_id, subject_name, teacher_name, topic_id):
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.id,
                    c.content_id AS lecture_id,
                    c.content_name AS lecture_name,
                    c.sub_topic_name,
                    t.topic_id,
                    t.topic_name,
                    COALESCE(s.subject_name, 'Unknown Subject') AS subject_name,
                    COALESCE(k.teacher_name, 'Unknown Teacher') AS teacher_name,
                    c.source_url,
                    c.file_path,
                    c.file_size,
                    c.ia_identifier,
                    c.ia_url,
                    c.status,
                    c.created_at,
                    c.updated_at
                FROM khazana_contents c
                JOIN khazana_topics t ON t.id = c.topic_id
                JOIN khazana_programs p ON p.id = t.program_id
                LEFT JOIN khazana_subjects s ON s.id = t.subject_id
                LEFT JOIN khazana_teachers k ON k.id = t.teacher_id
                WHERE p.program_id = %s
                  AND COALESCE(s.subject_name, 'Unknown Subject') = %s
                  AND COALESCE(k.teacher_name, 'Unknown Teacher') = %s
                                    AND t.topic_id = %s
                  AND c.content_type = 'lecture'
                ORDER BY
                    COALESCE(c.sub_topic_name, '') ASC,
                    COALESCE(c.content_name, '') ASC,
                    c.created_at ASC,
                    c.id ASC
                """,
                                (program_id, subject_name, teacher_name, topic_id),
            )
            rows = cur.fetchall() or []
    finally:
        conn.close()

    sequence_rows = []
    for idx, row in enumerate(rows, start=1):
        normalized = dict(row)
        normalized["sequence"] = idx
        for dt_key in ("created_at", "updated_at"):
            if isinstance(normalized.get(dt_key), datetime):
                normalized[dt_key] = normalized[dt_key].isoformat()
        sequence_rows.append(normalized)
    return sequence_rows


def main():
    db = _init_db()
    if not db:
        print("ERROR: Database not available. Set PWDL_DB_URL first.")
        sys.exit(1)

    batches = _list_batches_from_db(db)
    if not batches:
        print("No Khazana lecture batches found in DB.")
        sys.exit(1)

    selected_batch = _pick_one(
        batches,
        "Select batch:",
        lambda x: f"{x['batch_name']} [program={x['program_id']}]",
    )
    if not selected_batch:
        print("No batch selected.")
        sys.exit(1)

    program_id = selected_batch["program_id"]
    subjects = _get_subjects_for_program(db, program_id)
    if not subjects:
        print("No Khazana lectures found in DB for this batch/program.")
        sys.exit(1)

    selected_subject = _pick_one(subjects, "Select Khazana subject:", lambda x: x)
    if not selected_subject:
        print("No subject selected.")
        sys.exit(1)

    teachers = _get_teachers_for_subject(db, program_id, selected_subject)
    if not teachers:
        print("No teachers found for selected subject in DB.")
        sys.exit(1)

    selected_teacher = _pick_one(teachers, "Select teacher:", lambda x: x)
    if not selected_teacher:
        print("No teacher selected.")
        sys.exit(1)

    topics = _get_topics_for_teacher_subject(db, program_id, selected_subject, selected_teacher)
    if not topics:
        print("No topics found for selected subject + teacher.")
        sys.exit(1)

    selected_topic = _pick_one(
        topics,
        "Select topic:",
        lambda x: f"{x['topic_name']} ({x['topic_id']})",
    )
    if not selected_topic:
        print("No topic selected.")
        sys.exit(1)

    lectures = _get_lectures_sequence(
        db,
        program_id,
        selected_subject,
        selected_teacher,
        selected_topic["topic_id"],
    )
    if not lectures:
        print("No lectures found for selected subject + teacher + topic.")
        sys.exit(1)

    selected_lecture = _pick_one(
        lectures,
        "Select lecture (sequence wise):",
        lambda x: f"#{x['sequence']} | {x.get('lecture_name') or x.get('lecture_id')} | sub-topic={x.get('sub_topic_name') or '-'} | status={x.get('status')}",
    )

    result = {
        "source": "khazana_only",
        "mode": "database_only",
        "batch": selected_batch,
        "subject": selected_subject,
        "teacher": selected_teacher,
        "topic": selected_topic,
        "total_lectures": len(lectures),
        "lectures_sequence": lectures,
        "selected_lecture": selected_lecture,
        "note": "Use this output for frontend AI. It contains Khazana lectures only, separated from course lectures.",
    }

    print("\n===== KHAZANA ACCESS RESULT =====")
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
