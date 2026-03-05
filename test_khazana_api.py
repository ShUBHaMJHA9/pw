#!/usr/bin/env python3
"""
Diagnostic script to test Khazana API token and MPD fetching.
Tests token validity, API connectivity, and identifies 403 errors.
"""

import json
import os
import requests
import sys
from urllib.parse import urlparse

# Add the workspace to path
sys.path.insert(0, '/workspaces/pw')

from mainLogic.utils.glv_var import PREFS_FILE, debugger
from mainLogic.utils.Endpoint import Endpoint
from beta.batch_scraper_2.Endpoints import Endpoints

def load_token_config():
    """Load token from preferences."""
    try:
        with open(PREFS_FILE, 'r') as f:
            prefs = json.load(f)
            
        # Extract token from various possible locations
        users = prefs.get('users', [])
        token_config = prefs.get('token_config', {})
        
        if users and isinstance(users, list):
            # Get last used or first user
            user = users[0]
            token = user.get('access_token') or user.get('token')
            random_id = user.get('random_id') or user.get('randomId')
            return token, random_id
        elif token_config:
            token = token_config.get('access_token') or token_config.get('token')
            random_id = token_config.get('random_id') or token_config.get('randomId')
            return token, random_id
        else:
            return None, None
    except Exception as e:
        debugger.error(f"Failed to load token config: {e}")
        return None, None

def test_token_with_curl(token, random_id):
    """Test token with direct curl-like request to MPD endpoint."""
    debugger.info("=" * 70)
    debugger.info("Testing Token with Sample MPD Request")
    debugger.info("=" * 70)
    
    # Sample Khazana video ID and batch
    test_lecture_id = "680c85b0c9d776d19b869d3f"
    test_batch = "65d75d320531c20018ade9bb"
    
    if not token:
        debugger.error("No token found!")
        return False
    
    debugger.info(f"Token (first 20 chars): {token[:20]}...")
    debugger.info(f"Random ID: {random_id}")
    
    # First get the video signed URL
    try:
        debugger.info("\n[STEP 1] Getting signed URL from API...")
        ep = Endpoints(verbose=False).set_token(token, random_id=random_id)
        result = ep.process("lecture", lecture_id=test_lecture_id, batch_name=test_batch)
        
        if not result:
            debugger.error("Failed to get signed URL")
            return False
            
        signed_url = result.get('url') or result.get('data', {}).get('url')
        if not signed_url:
            debugger.error(f"No URL in response: {result}")
            return False
            
        debugger.success(f"Got signed URL: {signed_url[:100]}...")
        
        # Now test the MPD fetch
        debugger.info("\n[STEP 2] Fetching MPD with Bearer token...")
        headers_with_token = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/dash+xml",
            "Referer": "https://www.pw.live/",
            "Origin": "https://www.pw.live",
            "Authorization": f"Bearer {token}"
        }
        
        endpoint = Endpoint(url=signed_url, method='GET', headers=headers_with_token)
        response, status_code, headers = endpoint.fetch()
        
        debugger.info(f"Status Code: {status_code}")
        debugger.info(f"Response Headers: {dict(headers)}")
        
        if status_code == 403:
            debugger.error("❌ RECEIVED 403 FORBIDDEN - Authorization issue")
            debugger.info("\nResponse snippet:")
            if isinstance(response, str):
                debugger.info(response[:500])
            return False
        elif status_code == 200:
            debugger.success("✓ Successfully fetched MPD with token")
            if isinstance(response, str) and len(response) > 50:
                debugger.info(f"Response length: {len(response)} bytes")
                # Try to find KID
                import re
                match = re.search(r'default_KID="([0-9a-fA-F-]+)"', response)
                if match:
                    debugger.success(f"✓ Found KID: {match.group(1)}")
            return True
        else:
            debugger.warning(f"Unexpected status code: {status_code}")
            return False
            
    except Exception as e:
        debugger.error(f"Exception during test: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_endpoint_directly(token, random_id):
    """Test the Endpoint class directly with different headers."""
    debugger.info("\n" + "=" * 70)
    debugger.info("Direct Endpoint Test")
    debugger.info("=" * 70)
    
    test_url = "https://d1d6v9k87fquyj.cloudfront.net/video-content/63dcd89b40e5dc0019b78a11/master.mpd"
    
    # Test with different header combinations
    header_configs = [
        ("Bearer Token Only", {"Authorization": f"Bearer {token}"}),
        ("No Auth", {}),
    ]
    
    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/dash+xml",
    }
    
    for name, extra_headers in header_configs:
        debugger.info(f"\nTesting: {name}")
        headers = {**base_headers, **extra_headers}
        
        try:
            endpoint = Endpoint(url=test_url, method='GET', headers=headers)
            response, status_code, resp_headers = endpoint.fetch()
            
            debugger.info(f"  Status: {status_code}")
            if status_code != 200:
                debugger.warning(f"  Response: {str(response)[:100]}")
        except Exception as e:
            debugger.error(f"  Error: {e}")

def main():
    debugger.info("Starting Khazana API Diagnostic...")
    
    token, random_id = load_token_config()
    
    if not token:
        debugger.error("\n❌ NO TOKEN FOUND IN PREFERENCES!")
        debugger.info("Please ensure you have logged in and have a valid token.")
        debugger.info(f"Token file location: {PREFS_FILE}")
        return
    
    debugger.success(f"\n✓ Token loaded from: {PREFS_FILE}")
    
    success = test_token_with_curl(token, random_id)
    
    if not success:
        debugger.error("\n" + "=" * 70)
        debugger.error("DIAGNOSIS: Token API Test Failed")
        debugger.error("=" * 70)
        debugger.error("\nPossible causes:")
        debugger.error("1. Token has expired - need to re-login")
        debugger.error("2. Token is invalid - verify token format in preferences.json")
        debugger.error("3. API endpoint changed - contact support")
        debugger.error("4. Network/proxy issue - check connectivity")
    else:
        debugger.success("\n" + "=" * 70)
        debugger.success("✓ TOKEN VALIDATION SUCCESSFUL")
        debugger.success("=" * 70)

if __name__ == '__main__':
    main()
