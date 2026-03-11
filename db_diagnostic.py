#!/usr/bin/env python3
"""Diagnostic script to check database content and test flow."""

from mainLogic.utils.mysql_logger import _connect, ensure_schema

# Setup database
ensure_schema()

BATCH_ID = "68aed26dfa4435a1a092e653"
TEST_ID = "6996030dbe8952f40964c03e"

print("=" * 80)
print("DATABASE DIAGNOSTIC")
print("=" * 80)

conn = _connect()
with conn.cursor() as cur:
    # Check overall counts
    cur.execute("SELECT COUNT(*) as cnt FROM tests")
    total_tests = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM test_questions")
    total_questions = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM test_options")
    total_options = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM test_assets")
    total_assets = cur.fetchone()['cnt']
    
    print(f"\n📊 Total Records in Database:")
    print(f"  - Tests: {total_tests}")
    print(f"  - Questions: {total_questions}")
    print(f"  - Options: {total_options}")
    print(f"  - Assets: {total_assets}")
    
    # Check specific test
    print(f"\n🔍 Checking Test: {TEST_ID}")
    print(f"   Batch: {BATCH_ID}")
    
    cur.execute(
        "SELECT id, test_name, status, error_text, created_at FROM tests WHERE batch_id=%s AND test_id=%s",
        (BATCH_ID, TEST_ID)
    )
    test_row = cur.fetchone()
    
    if not test_row:
        print(f"   ❌ Test NOT in database")
    else:
        print(f"   ✓ Found in database!")
        print(f"     - Name: {test_row['test_name']}")
        print(f"     - Status: {test_row['status']}")
        if test_row['error_text']:
            print(f"     - Error: {test_row['error_text']}")
        print(f"     - Created: {test_row['created_at']}")
        
        test_id_db = test_row['id']
        
        # Check questions for this test
        cur.execute(
            "SELECT COUNT(*) as cnt FROM test_questions WHERE batch_id=%s AND test_id=%s",
            (BATCH_ID, TEST_ID)
        )
        q_cnt = cur.fetchone()['cnt']
        print(f"\n   Questions: {q_cnt}")
        
        # Check if status is done but no questions - that's the bug
        if test_row['status'] == 'done' and q_cnt == 0:
            print("\n   ⚠️  WARNING: Test marked as 'done' but has NO data!")
            print("       This indicates the download completed but nothing was stored.")
    
    # Show all tests in that batch
    print(f"\n📋 All tests in this batch:")
    cur.execute(
        "SELECT test_id, test_name, status, error_text FROM tests WHERE batch_id=%s ORDER BY created_at DESC LIMIT 5",
        (BATCH_ID,)
    )
    
    for row in cur.fetchall():
        status_emoji = "✓" if row['status'] == 'done' else "⏳" if row['status'] == 'downloading' else "❌"
        print(f"  {status_emoji} {row['test_id'][:20]}... | {row['test_name'][:30]:<30} | {row['status']:<12}")
        if row['error_text']:
            print(f"     Error: {row['error_text'][:50]}")

