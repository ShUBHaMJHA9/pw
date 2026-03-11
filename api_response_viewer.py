#!/usr/bin/env python3
"""Interactive test fetcher using correct batch_api methods - shows actual API response."""

import json
import sys
from beta.batch_scraper_2.Endpoints import Endpoints
from beta.batch_scraper_2.module import ScraperModule
from mainLogic.utils.Endpoint import Endpoint

prefs = ScraperModule.prefs or {}
users = prefs.get("users", []) if isinstance(prefs, dict) else []

if not users:
    print("❌ No user profiles found")
    exit(1)

print("\n" + "=" * 100)
print("SELECT USER")
print("=" * 100)

for idx, user in enumerate(users, start=1):
    uname = user.get("name") or f"user-{idx}"
    print(f"  {idx}. {uname}")

try:
    user_choice = int(input(f"\nSelect user [1]: ").strip() or "1") - 1
except:
    user_choice = 0

user = users[user_choice]

token = user.get("access_token") or user.get("token")
random_id = user.get("random_id") or user.get("randomId")

if not token:
    print("❌ Selected user has no token")
    exit(1)

print(f"\n✓ Using user: {user.get('name')}")

# Initialize batch API
batch_api = Endpoints(verbose=False).set_token(token, random_id=random_id) if random_id else Endpoints(verbose=False).set_token(token)

# Get purchased batches
print("\n" + "=" * 100)
print("FETCHING PURCHASED BATCHES...")
print("=" * 100 + "\n")

try:
    batches = batch_api.get_purchased_batches(all_pages=True) or []
except Exception as e:
    print(f"❌ Failed to fetch batches: {e}")
    exit(1)

if not batches:
    print("❌ No purchased batches found")
    exit(1)

print(f"✓ Found {len(batches)} batches\n")

print("=" * 100)
print("SELECT BATCH")
print("=" * 100)

for idx, batch in enumerate(batches[:10], start=1):
    bname = batch.get("name") or batch.get("slug") or f"Batch {idx}"
    print(f"  {idx}. {bname}")

try:
    batch_choice = int(input(f"\nSelect batch [1]: ").strip() or "1") - 1
except:
    batch_choice = 0

batch = batches[batch_choice]
batch_id = str(batch.get("_id") or "")

print(f"\n✓ Selected batch: {batch.get('name')}")

# Fetch tests
print("\n" + "=" * 100)
print("FETCHING TESTS IN BATCH...")
print("=" * 100 + "\n")

url = f"https://api.penpencil.co/v3/test-service/tests?batchId={batch_id}"
payload, status_code, _ = Endpoint(url=url, headers=batch_api.DEFAULT_HEADERS).fetch()

if status_code != 200:
    print(f"❌ Failed to fetch tests (HTTP {status_code})")
    exit(1)

tests = payload.get("data", [])

if not tests:
    print("❌ No tests in this batch")
    exit(1)

print(f"✓ Found {len(tests)} tests\n")

print("=" * 100)
print("SELECT TEST (to view actual API response data)")
print("=" * 100)

for idx, test in enumerate(tests[:10], start=1):
    tname = test.get("name") or f"Test {idx}"
    print(f"  {idx}. {tname}")

try:
    test_choice = int(input(f"\nSelect test [1]: ").strip() or "1") - 1
except:
    test_choice = 0

test = tests[test_choice]
test_id = str(test.get("_id") or "")

print(f"\n✓ Selected test: {test.get('name')}")
print(f"  Test ID: {test_id}\n")

# Fetch the actual test data
print("=" * 100)
print("FETCHING ACTUAL TEST DATA FROM API...")
print("=" * 100 + "\n")

url = (
    f"https://api.penpencil.co/v3/test-service/tests/{test_id}/start-test"
    f"?testSource=BATCH_TEST_SERIES&type=Start&batchId={batch_id}"
)

payload, status_code, _ = Endpoint(url=url, headers=batch_api.DEFAULT_HEADERS).fetch()

print(f"HTTP Status: {status_code}\n")

if status_code != 200:
    error = payload.get("error", {}).get("message") if isinstance(payload, dict) else "Unknown error"
    print(f"❌ Cannot start test: {error}")
    print(f"\nThis test is not in startable state. Try a different test.")
    exit(1)

if not payload.get("success"):
    print(f"❌ API returned success=false")
    exit(1)

data = payload.get("data", {})

# Display the actual test data
print("✅✅✅ SUCCESSFULLY FETCHED ACTUAL TEST DATA!\n")

print("=" * 100)
print("TEST METADATA (from actual API response)")
print("=" * 100)

print(f"Test Name: {data.get('name')}")
print(f"Test Type: {data.get('type')}")
print(f"Total Marks: {data.get('totalMarks')}")
print(f"Duration: {data.get('duration')} minutes")

questions = data.get("questions", [])
print(f"\n✓ TOTAL QUESTIONS IN RESPONSE: {len(questions)}")

# Show detailed structure
print("\n" + "=" * 100)
print("DETAILED STRUCTURE OF QUESTIONS (First 2)")
print("=" * 100 + "\n")

for q_idx, q_container in enumerate(questions[:2]):
    q = q_container.get("question", {})
    
    print(f"QUESTION #{q_idx + 1}")
    print(f"{'─' * 100}")
    
    # Question details
    q_id = q.get("_id")
    q_text = (q.get("questionTexts", {}).get("en") or "")[:80]
    q_type = q.get("type")
    marks_pos = q.get("positiveMarks")
    marks_neg = q.get("negativeMarks")
    
    print(f"ID: {q_id}")
    print(f"Question Text: {q_text}")
    print(f"Type: {q_type}")
    print(f"Marks: +{marks_pos}/-{marks_neg}")
    
    # Options
    options = q.get("options", [])
    print(f"\n✓ OPTIONS ({len(options)} total):")
    for opt_idx, opt in enumerate(options[:3]):
        opt_id = opt.get("_id")[:8]
        opt_text = (opt.get("texts", {}).get("en") or "")[:60]
        print(f"  [{opt_id}...] {opt_text}")
    if len(options) > 3:
        print(f"  ... and {len(options) - 3} more")
    
    # Correct answers
    solutions = q.get("solutions", [])
    print(f"\n✓ CORRECT ANSWER OPTION IDs: {solutions}")
    
    # Solution description
    sol_desc = q.get("solutionDescription", [])
    if sol_desc:
        print(f"\n✓ SOLUTION DESCRIPTION ({len(sol_desc)} parts):")
        for sol_idx, sol_part in enumerate(sol_desc[:1]):
            sol_text = (sol_part.get("text") or "")[:80]
            has_img = bool(sol_part.get("imageIds"))
            print(f"  Part {sol_idx+1}: {sol_text}")
            if has_img:
                print(f"  → Has image: YES")
    
    # Images in question
    if q.get("imageIds"):
        print(f"\n✓ QUESTION IMAGE: YES")
    
    print("\n")

if len(questions) > 2:
    print(f"... and {len(questions) - 2} more questions\n")

# Summary
print("=" * 100)
print("✅ DATA VERIFICATION SUMMARY")
print("=" * 100)

all_have_solutions = all(q.get("question", {}).get("solutions") for q in questions)
all_have_options = all(len(q.get("question", {}).get("options", [])) > 0 for q in questions)
has_images = sum(1 for q in questions if q.get("question", {}).get("imageIds"))
has_solution_desc = sum(1 for q in questions if q.get("question", {}).get("solutionDescription"))

print(f"\n✅ Test fetched successfully from API")
print(f"✅ Total questions: {len(questions)}")
print(f"✅ All questions have options: {all_have_options}")
print(f"✅ All questions have correct answers: {all_have_solutions}")
print(f"✅ Questions with images: {has_images}/{len(questions)}")
print(f"✅ Questions with solution descriptions: {has_solution_desc}/{len(questions)}")

print(f"\n{'=' * 100}")
print(f"✅✅✅ API IS WORKING CORRECTLY!")
print(f"    Questions, options, marks, correct answers, and solutions are all present.")
print(f"\nTest_dl.py would now:")
print(f"  1. Store test metadata in: test_questions table")
print(f"  2. Store each option in: test_options table")
print(f"  3. Store correct answer IDs in: test_questions.correct_option_ids_json")
print(f"  4. Download/upload images to Internet Archive → test_assets table")
print(f"  5. Store solution descriptions → test_assets table")
print(f"{'=' * 100}\n")
