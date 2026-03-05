#!/usr/bin/env python3
"""
Comprehensive diagnostic for Khazana download flow.
Traces from lecture selection to MPD/key fetch.
"""

import json
import sys
sys.path.insert(0, '/workspaces/pw')

from mainLogic.utils.glv_var import PREFS_FILE, debugger
from beta.batch_scraper_2.module import ScraperModule
from beta.batch_scraper_2.Endpoints import Endpoints
from mainLogic.big4.Ravenclaw_decrypt.key import LicenseKeyFetcher

def test_khazana_flow():
    """Test the complete Khazana flow."""
    
    # Load preferences
    try:
        with open(PREFS_FILE, 'r') as f:
            prefs = json.load(f)
    except Exception as e:
        debugger.error(f"Failed to load prefs: {e}")
        return
    
    users = prefs.get('users', [])
    if not users:
        debugger.error("No users found in preferences")
        return
    
    user = users[0]
    token = user.get('access_token') or user.get('token')
    random_id = user.get('random_id') or 'a3e290fa-ea36-4012-9124-8908794c33aa'
    
    debugger.info("=" * 70)
    debugger.info("Khazana Full Flow Diagnostic")
    debugger.info("=" * 70)
    debugger.info(f"User: {user.get('name')}")
    debugger.info(f"Token: {token[:30]}...")
    debugger.info(f"RandomID: {random_id}")
    
    # Test 1: Initialize LicenseKeyFetcher
    debugger.info("\n[STEP 1] Initializing LicenseKeyFetcher...")
    try:
        fetcher = LicenseKeyFetcher(token, random_id)
        debugger.success("✓ LicenseKeyFetcher initialized")
    except Exception as e:
        debugger.error(f"Failed to init fetcher: {e}")
        return
    
    # Test 2: Get signed URL for known test video
    debugger.info("\n[STEP 2] Testing get_video_signed_url()...")
    # IDs from verified /v2/programs/contents response sample
    test_id = "6535485ead923600182c2c5a"  # lecture _id (childId)
    test_batch = "653543ca81e74c00187aff4e"  # programId _id (parentId)
    test_topic = "653543ec97d69c0018883a22"  # subjectDetails _id (secondaryParentId)
    test_video_id = "637cfc0f1b201a001163c14a"  # videoDetails.id (videoId)
    test_url = "https://d1d34p8vz63oiq.cloudfront.net/8f68d9f0-c536-44be-bd1c-0e78add809dc/master.mpd"
    
    try:
        signed_url = fetcher.get_video_signed_url(
            id=test_id,
            batch_name=test_batch,
            khazana_topic_name=test_topic,
            khazana_url=test_url,
            video_id=test_video_id,
            verbose=True
        )
        
        if signed_url:
            debugger.success(f"✓ Got signed URL: {signed_url[:100]}...")
        else:
            debugger.error("❌ get_video_signed_url returned None")
            return
            
    except Exception as e:
        debugger.error(f"❌ get_video_signed_url failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 3: Extract KID from MPD
    debugger.info("\n[STEP 3] Testing extract_kid_from_mpd()...")
    try:
        kid = fetcher.extract_kid_from_mpd(signed_url)
        if kid:
            debugger.success(f"✓ Got KID: {kid}")
        else:
            debugger.error("❌ extract_kid_from_mpd returned None/empty")
            return
    except Exception as e:
        debugger.error(f"❌ extract_kid_from_mpd failed: {e}")
        debugger.error("\nDiagnosis:")
        debugger.error("- This is likely a 403 error from CloudFront")
        debugger.error("- Possible causes:")
        debugger.error("  1. Signed URL has expired")
        debugger.error("  2. CloudFront distribution requires specific headers")
        debugger.error("  3. Token/auth header is being rejected")
        return
    
    # Test 4: Get full key
    debugger.info("\n[STEP 4] Testing full get_key() flow...")
    try:
        result = fetcher.get_key(
            id=test_id,
            batch_name=test_batch,
            khazana_topic_name=test_topic,
            khazana_url=test_url,
            video_id=test_video_id,
            verbose=True
        )
        
        if result:
            kid, key, url = result
            debugger.success(f"✓ Got key result:")
            debugger.success(f"  KID: {kid}")
            debugger.success(f"  Key: {key[:20]}..." if key else "  Key: None")
            debugger.success(f"  URL: {url[:100]}...")
        else:
            debugger.error("❌ get_key returned None")
            
    except Exception as e:
        debugger.error(f"❌ get_key failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_khazana_flow()
