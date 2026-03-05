#!/usr/bin/env python3
"""
Token refresh utility for Khazana API.
Handles token expiration and automatic refresh using refresh_token.
"""

import json
import requests
import time
import sys
import os

sys.path.insert(0, '/workspaces/pw')

from mainLogic.utils.glv_var import PREFS_FILE, debugger

class TokenRefreshManager:
    """Manages token lifecycle and automatic refresh."""
    
    TOKEN_URL = "https://api.penpencil.co/v3/oauth/token"
    
    def __init__(self, prefs_file=PREFS_FILE):
        self.prefs_file = prefs_file
        self.prefs = None
        self.load_prefs()
    
    def load_prefs(self):
        """Load preferences from file."""
        try:
            with open(self.prefs_file, 'r') as f:
                self.prefs = json.load(f)
        except Exception as e:
            debugger.error(f"Failed to load preferences: {e}")
            self.prefs = {}
    
    def save_prefs(self):
        """Save preferences to file."""
        try:
            with open(self.prefs_file, 'w') as f:
                json.dump(self.prefs, f, indent=2)
            debugger.success("Preferences saved")
        except Exception as e:
            debugger.error(f"Failed to save preferences: {e}")
    
    def is_token_expired(self, user_idx=0):
        """Check if token is expired."""
        users = self.prefs.get('users', [])
        if user_idx >= len(users):
            return True
        
        user = users[user_idx]
        token_data = user.get('token', {})
        
        if isinstance(token_data, dict):
            expires_in = token_data.get('expires_in')
            if expires_in:
                # expires_in is usually a timestamp
                current_time = int(time.time() * 1000)
                is_expired = current_time > expires_in
                if is_expired:
                    debugger.warning(f"Token expired at {expires_in}, current time: {current_time}")
                return is_expired
        
        return False
    
    def refresh_access_token(self, user_idx=0):
        """Refresh access token using refresh_token."""
        users = self.prefs.get('users', [])
        if user_idx >= len(users):
            debugger.error("Invalid user index")
            return False
        
        user = users[user_idx]
        token_data = user.get('token', {})
        refresh_token = token_data.get('refresh_token')
        
        if not refresh_token:
            debugger.error("No refresh_token found")
            return False
        
        debugger.info(f"Refreshing token for user: {user.get('name', f'User {user_idx}')}")
        
        headers = {
            'accept': 'application/json, text/plain, */*',
            'client-id': '5eb393ee95fab7468a79d189',
            'client-type': 'WEB',
            'client-version': '201',
            'content-type': 'application/json',
        }
        
        payload = {
            'refreshToken': refresh_token,
            'grantType': 'refresh_token'
        }
        
        try:
            response = requests.post(self.TOKEN_URL, json=payload, headers=headers, timeout=10)
            
            if response.status_code == 200:
                new_token_data = response.json()
                
                # Update token in preferences
                if new_token_data.get('success') or new_token_data.get('data'):
                    data = new_token_data.get('data', new_token_data)
                    
                    # Update access_token fields
                    new_access_token = data.get('accessToken') or data.get('access_token')
                    if new_access_token:
                        user['access_token'] = new_access_token
                        if isinstance(user.get('token'), dict):
                            user['token']['access_token'] = new_access_token
                        self.prefs['token'] = new_access_token  # Also update root token
                    
                    # Update expires_in
                    new_expires = data.get('expiresIn') or data.get('expires_in')
                    if new_expires and isinstance(user.get('token'), dict):
                        user['token']['expires_in'] = new_expires
                    
                    self.save_prefs()
                    debugger.success("✓ Token refreshed successfully")
                    return True
                else:
                    debugger.error(f"Unexpected response: {new_token_data}")
                    return False
            else:
                debugger.error(f"Token refresh failed: {response.status_code}")
                debugger.error(f"Response: {response.text[:200]}")
                return False
                
        except Exception as e:
            debugger.error(f"Token refresh exception: {e}")
            return False
    
    def ensure_valid_token(self, user_idx=0):
        """Ensure token is valid, refresh if needed."""
        if self.is_token_expired(user_idx):
            return self.refresh_access_token(user_idx)
        return True

def check_and_refresh_token():
    """Check token validity and refresh if needed."""
    manager = TokenRefreshManager()
    
    debugger.info("=" * 70)
    debugger.info("Token Validation Check")
    debugger.info("=" * 70)
    
    if manager.is_token_expired(0):
        debugger.warning("Token is expired or about to expire")
        if manager.refresh_access_token(0):
            debugger.success("Token refresh successful - ready to download")
            return True
        else:
            debugger.error("Token refresh failed - you need to re-login")
            return False
    else:
        debugger.success("Token is valid")
        return True

if __name__ == '__main__':
    check_and_refresh_token()
