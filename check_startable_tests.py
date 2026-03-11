#!/usr/bin/env python3
"""Check which tests are in startable state."""

import sys
from beta.batch_scraper_2.Endpoints import Endpoints
from beta.batch_scraper_2.module import ScraperModule
from mainLogic.utils.Endpoint import Endpoint

prefs = ScraperModule.prefs or {}
users = prefs.get("users", []) if isinstance(prefs, dict) else []

if not users:
    print("❌ No users configured")
    sys.exit(1)

print("\nUsers available:")
for idx, user in enumerate(users):
    name = user.get("name") or f"user-{idx+1}"
    print(f"  {idx+1}. {name}")

try:
    user_choice = int(input(f"\nSelect user [1]: ").strip() or "1") - 1
except:
    user_choice = 0

user = users[user_choice]
token = user.get("access_token") or user.get("token")
random_id = user.get("random_id") or user.get("randomId")

if not token:
    print("❌ Selected user has no token")
    sys.exit(1)

batch_api = Endpoints(verbose=False).set_token(token, random_id=random_id) if random_id else Endpoints(verbose=False).set_token(token)

print("\n" + "=" * 100)
print("CHECKING ALL TESTS FOR STARTABLE STATE...")
print("=" * 100 + "\n")

batches = batch_api.get_purchased_batches(all_pages=True) or []

startable_count = 0
not_startable_count = 0
startable_tests = []

for batch in batches:
    batch_id = str(batch.get("_id") or "")
    batch_name = batch.get("name", "Unknown")[:60]
    
    url = f"https://api.penpencil.co/v3/test-service/tests?batchId={batch_id}"
    payload, status, _ = Endpoint(url=url, headers=batch_api.DEFAULT_HEADERS).fetch()
    
    if status != 200:
        continue
    
    tests = payload.get("data", [])
    
    print(f"\n📚 Batch: {batch_name}")
    print(f"   Tests: {len(tests)}\n")
    
    for test_idx, test in enumerate(tests):
        test_id = str(test.get("_id") or "")
        test_name = (test.get("name") or "Unknown")[:50]
        
        start_url = f"https://api.penpencil.co/v3/test-service/tests/{test_id}/start-test?testSource=BATCH_TEST_SERIES&type=Start&batchId={batch_id}"
        resp, st, _ = Endpoint(url=start_url, headers=batch_api.DEFAULT_HEADERS).fetch()
        
        if st == 200 and resp.get("success"):
            print(f"   ✅ {test_name:45} CAN START ")
            startable_count += 1
            startable_tests.append({
                "name": test_name,
                "test_id": test_id,
                "batch_name": batch_name,
                "batch_id": batch_id
            })
        else:
            error = "?"
            if isinstance(resp, dict) and resp.get("error"):
                error = resp.get("error", {}).get("message", "?")[:30]
            print(f"   ❌ {test_name:45} {error}")
            not_startable_count += 1

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"✅ Startable tests: {startable_count}")
print(f"❌ Not startable: {not_startable_count}")
print(f"📊 Total: {startable_count + not_startable_count}")

if startable_tests:
    print(f"\n{'=' * 100}")
    print("AVAILABLE TESTS YOU CAN DOWNLOAD:")
    print("=" * 100)
    for idx, test in enumerate(startable_tests[:10]):
        print(f"\n{idx+1}. {test['name']}")
        print(f"   Batch: {test['batch_name']}")
        print(f"   Test ID: {test['test_id']}")
        print(f"   Batch ID: {test['batch_id']}")
    
    if len(startable_tests) > 10:
        print(f"\n... and {len(startable_tests) - 10} more")
else:
    print(f"\n⚠️  NO STARTABLE TESTS FOUND!")
    print(f"   All tests are either:")
    print(f"   • Already started by student")
    print(f"   • Scheduled for future date")
    print(f"   • Archived/completed")
    print(f"\n   Try a different user profile or create new tests.")
