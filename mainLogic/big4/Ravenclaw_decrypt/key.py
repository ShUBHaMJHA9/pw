import re
import base64
import json
from beta.batch_scraper_2.Endpoints import Endpoints
from mainLogic.big4.Ravenclaw_decrypt.heck import get_cookiees_from_url
from mainLogic.big4.obsolete.Obsolete_Gryffindor_downloadv2 import Download
from mainLogic.utils.glv import Global
from mainLogic.utils.glv_var import debugger
from mainLogic.utils.keyUtils import cookies_dict_to_str
from mainLogic.utils.Endpoint import Endpoint

class LicenseKeyFetcher:
    def __init__(self, token, random_id):
        self.url = None
        self.token = token
        self.random_id = random_id
        self.cookies = None
        self._load_khazana_cookies()
    
    def _load_khazana_cookies(self):
        """Load CloudFront cookies from preferences.json if available."""
        try:
            import os
            prefs_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'preferences.json')
            if os.path.exists(prefs_path):
                with open(prefs_path, 'r') as f:
                    prefs = json.load(f)
                    if 'khazana_cookies' in prefs and prefs['khazana_cookies']:
                        self.cookies = prefs['khazana_cookies']
        except Exception as e:
            # Silently fail if we can't load preferences
            pass

    def build_license_url(self, encoded_otp_key):
        return f"https://api.penpencil.co/v1/videos/get-otp?key={encoded_otp_key}&isEncoded=true"

    def get_video_signed_url(self, id, batch_name, khazana_topic_name, khazana_url, video_id=None, verbose=False):
        """
        Fetch signed URL for Khazana video using the video-url-details endpoint.
        This endpoint returns both 'url' and 'signedUrl' query parameters.
        """
        try:
            if verbose:
                debugger.debug("Calling Khazana video-url-details endpoint to get signed URL...")
            
            # Build device capability headers matching the actual working client request
            device_headers = {
                "Authorization": f"Bearer {self.token}",
                "Client-Id": "5eb393ee95fab7468a79d189",
                "Client-Type": "WEB",
                    "Client-Version": "200",
                "Randomid": str(self.random_id) if self.random_id else "a3e290fa-ea36-4012-9124-8908794c33aa",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate, br",
                "Origin": "https://www.pw.live",
                "Referer": "https://www.pw.live/",
                    "Sec-Ch-Ua": '"Chromium";v="130", "Brave";v="130", "Not?A_Brand";v="99"',
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
                    "Sec-Gpc": "1",
                    "Priority": "u=1, i",
                # Device capability headers (exact match with working request)
                "Networktype": "unknown",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Frameratecapability": '{"videoQuality":"720p (HD)"}',
                "Audiocodeccapability": '{"AAC-LC":{"isSupported":true,"Profile":[{"container":"audio/mp4","supported":true},{"container":"audio/webm","supported":false},{"container":"audio/ogg","supported":false}]},"HE-AAC v1":{"isSupported":true,"Profile":[{"container":"audio/mp4","supported":true},{"container":"audio/webm","supported":false},{"container":"audio/ogg","supported":false}]},"HE-AAC v2":{"isSupported":true,"Profile":[{"container":"audio/mp4","supported":true},{"container":"audio/webm","supported":false},{"container":"audio/ogg","supported":false}]}}',
                "Devicememory": "2048",
                "Devicestreamingtechnology": '{"dash":{"isSupported":true,"formats":["mp4","m4a"],"codecs":["avc1","aac"]},"hls":{"isSupported":false,"formats":[],"codecs":[]}}',
                "Screenresolution": "1539 x 833",
                "Devicetype": "desktop",
                "Drmcapability": '{"aesSupport":"yes","fairPlayDrmSupport":"no","playreadyDrmSupport":"no","widevineDRMSupport":"yes"}',
                "Videocodeccapability": '{"Hevc":{"isSupported":"true","Profile":[{"name":"Main"},{"name":"Main 10"},{"name":"Main 12"},{"name":"Main 4:2:2 10"},{"name":"Main 4:2:2 12"},{"name":"Main 4:4:4"},{"name":"Main 4:4:4 10"},{"name":"Main 4:4:4 12"},{"name":"Main 4:4:4 16 Intra"}]},"AV1":{"isSupported":"true","Profile":[{"name":"Main"},{"name":"High"},{"name":"Professional"}]}}',
                "Version": "0.0.1",
            }
            
            # Build the endpoint with all required parameters (reqType=query returns the signature)
            endpoint_url = (
                f"https://api.penpencil.co/v1/videos/video-url-details"
                f"?type=RECORDED&videoContainerType=DASH&reqType=query"
                f"&childId={id}&parentId={batch_name}"
                f"&videoUrl={khazana_url}&secondaryParentId={khazana_topic_name}&clientVersion=201"
            )
            
            if video_id:
                endpoint_url += f"&videoId={video_id}"
            
            if verbose:
                    debugger.debug(f"Khazana video-url-details endpoint (url truncated for security)")
                    debugger.debug(f"  type=RECORDED, childId={id}, parentId={batch_name}, secondaryParentId={khazana_topic_name}")
            
            endpoint = Endpoint(url=endpoint_url, method='GET', headers=device_headers)
            response, status_code, resp_obj = endpoint.fetch()
            
            if status_code != 200:
                # If request failed with video_id, try without it
                if video_id and status_code == 403:
                    if verbose:
                        debugger.debug(f"Request with videoId failed (403), retrying without videoId...")
                    endpoint_url_no_vid = (
                        f"https://api.penpencil.co/v1/videos/video-url-details"
                        f"?type=RECORDED&videoContainerType=DASH&reqType=query"
                        f"&childId={id}&parentId={batch_name}"
                        f"&videoUrl={khazana_url}&secondaryParentId={khazana_topic_name}"
                    )
                    endpoint = Endpoint(url=endpoint_url_no_vid, method='GET', headers=device_headers)
                    response, status_code, resp_obj = endpoint.fetch()
                
                # If still failing, try reqType=cookie as fallback
                if status_code != 200:
                    if verbose:
                        debugger.debug(f"Request with reqType=query failed, trying reqType=cookie...")
                    endpoint_url_cookie = (
                        f"https://api.penpencil.co/v1/videos/video-url-details"
                        f"?type=RECORDED&videoContainerType=DASH&reqType=cookie"
                        f"&childId={id}&parentId={batch_name}"
                        f"&videoUrl={khazana_url}&secondaryParentId={khazana_topic_name}"
                    )
                    if video_id:
                        endpoint_url_cookie += f"&videoId={video_id}"
                    endpoint = Endpoint(url=endpoint_url_cookie, method='GET', headers=device_headers)
                    response, status_code, resp_obj = endpoint.fetch()
                    
                if status_code != 200:
                    if verbose:
                        debugger.error(f"Failed to fetch signed URL. Status: {status_code}")
                        if isinstance(response, dict):
                            debugger.error(f"Response: {response}")
                    return None
            
            if isinstance(response, dict) and response.get('success') and 'data' in response:
                signed_data = response.get('data', {})
                if verbose:
                    debugger.debug(f"API response data keys: {list(signed_data.keys())}")

                # Extract base URL and signed query string (standard Khazana response format)
                base_url = signed_data.get('url')
                signed_query = signed_data.get('signedUrl', '')
                
                if base_url and signed_query:
                    # Combine base URL with signed query parameters
                    full_signed_url = f"{base_url}{signed_query}"
                    if verbose:
                        debugger.success(f"Combined URL from base + signedUrl query string")
                    return full_signed_url
                
                # Fallback: try other possible response keys for signed URL
                for key in ['signedUrl', 'url', 'masterUrl', 'playUrl', 'videoUrl']:
                    url_candidate = signed_data.get(key)
                    if url_candidate and isinstance(url_candidate, str):
                        if verbose:
                            debugger.success(f"Got signed URL from '{key}' field")
                        return url_candidate

                if verbose:
                    response_str = json.dumps(response, indent=2)[:500] if isinstance(response, dict) else str(response)[:500]
                    debugger.error(f"Unexpected response format or missing data:\n{response_str}")
                return None
            
        except Exception as e:
            if verbose:
                debugger.error(f"Error fetching signed URL: {e}")
            return None

    def get_otp_headers(self):
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,la;q=0.8",
            "authorization": f"Bearer {self.token}",
            "cache-control": "no-cache",
            "client-id": "5eb393ee95fab7468a79d189",
            "client-type": "WEB",
            "client-version": "200",
            "content-type": "application/json",
            "dnt": "1",
            "origin": "https://www.pw.live",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "randomid": "a3e290fa-ea36-4012-9124-8908794c33aa",
            "referer": "https://www.pw.live/",
            "sec-ch-ua": "\"Google Chrome\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }
        return headers

    def key_char_at(self, key, i):
        return ord(key[i % len(key)])

    def b64_encode(self, data):
        if not data:
            return data
        encoded = base64.b64encode(bytes(data)).decode('utf-8')
        return encoded

    def get_key_final(self, otp):
        decoded_bytes = base64.b64decode(otp)
        length = len(decoded_bytes)
        decoded_ints = [int(byte) for byte in decoded_bytes]

        result = "".join(
            chr(
                decoded_ints[i] ^ ord(self.token[i % len(self.token)])
            )
            for i in range(length)
        )

        return result

    def xor_encrypt(self, data):
        return [ord(c) ^ self.key_char_at(self.token, i) for i, c in enumerate(data)]

    def insert_zeros(self, hex_string):
        result = "00"
        for i in range(0, len(hex_string), 2):
            result += hex_string[i:i+2]
            if i + 2 < len(hex_string):
                result += "00"
        return result

    def extract_kid_from_mpd(self, url):
        # Extract CloudFront cookies/signature from the URL if present
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/dash+xml",
            "Referer": "https://www.pw.live/",
            "Origin": "https://www.pw.live",
        }
        
        # Add authorization token if token is available
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        
        # Add CloudFront cookies if we have them
        if self.cookies:
            headers["Cookie"] = self.cookies
        
        # Try with these headers first
        try:
            endpoint = Endpoint(url=url, method='GET', headers=headers)
            response, status_code, _ = endpoint.fetch()
            if status_code != 200:
                raise Exception(f"Failed to fetch MPD content. Status code: {status_code}")
            mpd_content = response
            pattern = r'default_KID="([0-9a-fA-F-]+)"'
            match = re.search(pattern, mpd_content)
            return match.group(1) if match else None
        except Exception as e:
            # If it fails with auth headers, try without them (in case CloudFront URL has signature embedded)
            try:
                endpoint = Endpoint(url=url, method='GET')
                response, status_code, _ = endpoint.fetch()
                if status_code != 200:
                    raise Exception(f"Failed to fetch MPD content. Status code: {status_code}")
                mpd_content = response
                pattern = r'default_KID="([0-9a-fA-F-]+)"'
                match = re.search(pattern, mpd_content)
                return match.group(1) if match else None
            except Exception:
                raise e

    def set_cookies(self, url):
        self.cookies = cookies_dict_to_str(get_cookiees_from_url(url))

    def get_key(self, id, batch_name,khazana_topic_name=None,khazana_url=None,video_id=None, verbose=True):
        if verbose: Global.hr()

        if verbose: debugger.debug("Beginning to get the key for the video... & Audio :) ")
        if verbose: debugger.debug(f"ID: {id}")
        if verbose: debugger.debug("Building the URL to get the key...")

        try:
            #from mainLogic.big4.Ravenclaw_decrypt.signedUrl import get_signed_url

            # policy_string = get_signed_url(token=self.token, random_id=self.random_id, id=id, verbose=verbose)['data']
            # add_on = cookie_splitter(policy_string, verbose)

            # If a signed Khazana master.mpd URL is provided, use it directly
            # Khazana already provides pre-signed CloudFront URLs in the chapter response
            if khazana_url:
                if verbose: debugger.debug("Processing Khazana URL (will fetch signed URL from API)...")
                
                # First, try to get the signed URL with proper credentials from the API endpoint
                signed_url = self.get_video_signed_url(
                    id=id, 
                    batch_name=batch_name,
                    khazana_topic_name=khazana_topic_name,
                    khazana_url=khazana_url,
                    video_id=video_id,
                    verbose=verbose
                )
                
                # If we got a signed URL, use it; otherwise fallback to the provided URL
                actual_url = signed_url if signed_url else khazana_url
                self.url = actual_url
                self.set_cookies(actual_url)

                try:
                    if verbose: debugger.debug("Extracting the KID from the provided MPD file...")
                    kid = self.extract_kid_from_mpd(self.url)
                    if not kid:
                        raise Exception("KID not found in MPD")
                    kid = kid.replace("-", "")
                    if verbose: debugger.success(f"KID: {kid}")

                    otp_key = self.b64_encode(self.xor_encrypt(kid))
                    encoded_otp_key_step1 = otp_key.encode('utf-8').hex()
                    encoded_otp_key = self.insert_zeros(encoded_otp_key_step1)
                    license_url = self.build_license_url(encoded_otp_key)
                    headers = self.get_otp_headers()
                    if self.cookies:
                        headers.setdefault('Cookie', self.cookies)

                    endpoint = Endpoint(url=license_url, method='GET', headers=headers)
                    response, status_code, _ = endpoint.fetch()
                    if status_code == 200 and isinstance(response, dict) and response.get('data', {}).get('otp'):
                        key = self.get_key_final(response['data']['otp'])
                        if verbose: debugger.success("Key received from Khazana CloudFront URL!")
                        return (kid, key, self.url)
                    else:
                        if verbose: debugger.debug(f"Failed to get key from license URL: status={status_code}")
                except Exception as e:
                    if verbose: debugger.error(f"Failed to process Khazana URL: {e}")

            if not khazana_topic_name and not khazana_url:
                # For non-Khazana content, use the standard batch endpoint
                url_op = Endpoints(verbose=True).set_token(self.token, self.random_id).process(
                    "lecture",
                    lecture_id=id,
                    batch_name=batch_name,
                )
            else:
                # For Khazana content, the URLs are already pre-signed in the chapter response
                # Skip the video-url-details endpoint call (it returns 403 for Khazana RECORDED type)
                if verbose:
                    debugger.debug(f"Khazana content detected: using pre-signed CloudFront URL directly")
                    debugger.debug(f"  lectureId={id}")
                    debugger.debug(f"  videoUrl={khazana_url[:80]}..." if len(khazana_url or '') > 80 else f"  videoUrl={khazana_url}")
                
                # Return the provided khazana_url as-is (it's already signed by CloudFront)
                url_op = {"url": khazana_url, "signedUrl": ""}
            if not isinstance(url_op, dict):
                if verbose:
                    debugger.error(f"Failed to fetch lecture URL. Response type: {type(url_op)}")
                    if khazana_topic_name:
                        debugger.error("=" * 70)
                        debugger.error("KHAZANA DOWNLOAD ISSUE DETECTED")
                        debugger.error("=" * 70)
                        debugger.error("The video-url-details API endpoint returned an unexpected response.")
                        debugger.error("")
                        debugger.error("Response received:")
                        debugger.error(f"  Type: {type(url_op)}")
                        debugger.error(f"  Value: {url_op}")
                        debugger.error("")
                        debugger.error("This may indicate:")
                        debugger.error("  - API response structure has changed")
                        debugger.error("  - Khazana content uses different API endpoints/format")
                        debugger.error("  - Token or authorization is invalid")
                        debugger.error("=" * 70)
                        try:
                            import time, json as _json
                            debug_obj = {
                                "lecture_id": id,
                                "program": batch_name,
                                "lecture_url": khazana_url,
                                "response_type": str(type(url_op)),
                                "response_value": str(url_op),
                                "timestamp": int(time.time()),
                            }
                            fn = f"/tmp/khazana_debug_{id}.json"
                            with open(fn, 'w') as _f:
                                _f.write(_json.dumps(debug_obj, indent=2))
                            if verbose:
                                debugger.info(f"Saved Khazana diagnostic to {fn}")
                        except Exception:
                            pass
                return None
            
            url = url_op.get('url') or url_op.get('data', {}).get('url')
            signature = url_op.get('signedUrl') or url_op.get('data', {}).get('signedUrl')
            
            if not url:
                if verbose:
                    debugger.error(f"Lecture URL response missing 'url' field.")
                    debugger.debug(f"Available keys in response: {list(url_op.keys())}")
                return None

            # signedUrl might be embedded in url or might be separate
            if signature:
                url = Download.buildUrl(url, signature)
            
            self.url = url
            self.set_cookies(url)

            if verbose: debugger.success(f"URL: {url[:100]}...")
            if verbose:
                Global.hr()
                debugger.success(f"Cookies: {self.cookies}")

            if verbose: debugger.debug("Extracting the KID from the MPD file...")
            kid = self.extract_kid_from_mpd(url).replace("-", "")
            if verbose: debugger.success(f"KID: {kid}")

            if verbose: debugger.debug("Encrypting the KID to get the key...")
            otp_key = self.b64_encode(self.xor_encrypt(kid))
            if verbose: debugger.success(f"OTP Key: {otp_key}")

            if verbose: debugger.debug("Encoding the OTP key to hex...")
            encoded_otp_key_step1 = otp_key.encode('utf-8').hex()
            encoded_otp_key = self.insert_zeros(encoded_otp_key_step1)
            if verbose: debugger.success(f"Encoded OTP Key: {encoded_otp_key}")

            if verbose: debugger.debug("Building the license URL...")
            license_url = self.build_license_url(encoded_otp_key)
            if verbose: debugger.success(f"License URL: {license_url}")

            if verbose: debugger.debug("Getting the headers...")
            headers = self.get_otp_headers()
            # Include cookies converted from the signed URL if available
            if self.cookies:
                headers.setdefault('Cookie', self.cookies)
            if verbose: debugger.success(f"Headers: {json.dumps(headers, indent=4)}")

            if verbose: debugger.debug("Making a request to the server to get the license (key)...")
            endpoint = Endpoint(url=license_url, method='GET', headers=headers)
            response, status_code, _ = endpoint.fetch()
            if verbose: debugger.success(f"Response: {response}")

            if status_code == 200:
                if 'data' in response and 'otp' in response['data']:
                    if verbose: debugger.success("Key received successfully!")
                    key = self.get_key_final(response['data']['otp'])
                    if verbose: debugger.success(f"Key: {key}")

                    if verbose:Global.hr()
                    return (kid,key,url)
            else:
                debugger.error("Could not get the key from the server. Exiting...")
                return None

        except Exception as e:
            debugger.error(f"An error occurred while getting the key: {e}")
            return None
