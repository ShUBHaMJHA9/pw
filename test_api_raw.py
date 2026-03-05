#!/usr/bin/env python3
"""
Enhanced diagnostic to check API endpoint responses with raw status codes.
"""

import json
import requests
import sys
sys.path.insert(0, '/workspaces/pw')

from mainLogic.utils.glv_var import PREFS_FILE, debugger

def load_token():
    with open(PREFS_FILE, 'r') as f:
        prefs = json.load(f)
    users = prefs.get('users', [])
    if users:
        user = users[0]
        token = user.get('access_token') or user.get('token')
        random_id = user.get('random_id') or user.get('randomId') or 'a3e290fa-ea36-4012-9124-8908794c33aa'
        return token, random_id
    return None, None

def test_api_directly():
    token, random_id = load_token()
    
    if not token:
        debugger.error("No token found")
        return
    
    # API URL for lecture endpoint
    url = "https://api.penpencil.co/v3/videos/video-url-details"
    params = {
        "type": "BATCHES",
        "childId": "680c85b0c9d776d19b869d3f",
        "parentId": "65d75d320531c20018ade9bb",
        "reqType": "query",
        "videoContainerType": "DASH"
    }
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'client-id': '5eb393ee95fab7468a79d189',
        'client-type': 'WEB',
        'client-version': '201',
        'content-type': 'application/json',
        'origin': 'https://www.pw.live',
        'referer': 'https://www.pw.live/',
        'user-agent': 'Mozilla/5.0',
        'x-sdk-version': '0.0.16',
        'randomid': random_id,
        'Authorization': f'Bearer {token}'
    }
    
    debugger.info("Testing API endpoint directly...")
    debugger.info(f"URL: {url}")
    debugger.info(f"Params: {params}")
    debugger.info(f"Auth header: Bearer {token[:30]}...")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        debugger.info(f"\n[RAW RESPONSE]")
        debugger.info(f"Status Code: {response.status_code}")
        debugger.info(f"Headers: {dict(response.headers)}")
        debugger.info(f"Response Text: {response.text[:500]}")
        
        if response.status_code == 200:
            debugger.success("✓ API call successful!")
            try:
                data = response.json()
                debugger.info(f"JSON Response keys: {list(data.keys())}")
                if 'success' in data:
                    debugger.info(f"Success: {data['success']}")
            except:
                pass
        elif response.status_code in [401, 403]:
            debugger.error(f"❌ Authorization failed (HTTP {response.status_code})")
            try:
                error_data = response.json()
                debugger.error(f"Error response: {json.dumps(error_data, indent=2)}")
            except:
                debugger.error(f"Response: {response.text}")
        else:
            debugger.warning(f"Unexpected status: {response.status_code}")
            
    except Exception as e:
        debugger.error(f"Exception: {e}")
        import traceback
        traceback.print_exc()

def test_without_auth():
    """Test the endpoint without Authorization header"""
    url = "https://api.penpencil.co/v3/videos/video-url-details"
    params = {
        "type": "BATCHES",
        "childId": "680c85b0c9d776d19b869d3f",
        "parentId": "65d75d320531c20018ade9bb",
    }
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'client-id': '5eb393ee95fab7468a79d189',
        'client-type': 'WEB',
        'user-agent': 'Mozilla/5.0',
    }
    
    debugger.info("\n" + "="*70)
    debugger.info("Testing WITHOUT Authorization header...")
    debugger.info("="*70)
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        debugger.info(f"Status: {response.status_code}")
        if response.status_code != 200:
            debugger.info(f"Response: {response.text[:300]}")
    except Exception as e:
        debugger.error(f"Exception: {e}")

if __name__ == '__main__':
    test_api_directly()
    test_without_auth()
