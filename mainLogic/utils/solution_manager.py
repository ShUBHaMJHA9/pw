#!/usr/bin/env python3
"""
Enhanced Solution Manager for Test Series

Handles:
1) Solutions with multiple images, videos, and description steps
2) CloudFront CDN video downloads with proper headers
3) Upload to Internet Archive and store IA identifiers
4) Professional database organization with test_solutions table
5) Proper mapping between questions, solutions, and assets

Database Flow:
  test_questions → test_solutions → test_assets (IA links)
  
Solution types:
  - description_step: Text + optional image (from solutionDescription array)
  - question_video: Video from question itself
  - result_video: Video from my-result endpoint
  - step_image: Image within a solution step
"""

import json
import os
import tempfile
import mimetypes
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse

import requests


class SolutionManager:
    """Manages solution extraction, download, upload, and database storage"""
    
    def __init__(self, db_logger, batch_api=None):
        """
        Initialize solution manager
        
        Args:
            db_logger: Database logger instance (mysql_logger)
            batch_api: Batch API instance for authenticated requests
        """
        self.db_logger = db_logger
        self.batch_api = batch_api
        self.ia_prefix = os.getenv("IA_IDENTIFIER_PREFIX") or "pw-test-series"
    
    # ========================================================================
    # SOLUTION EXTRACTION
    # ========================================================================
    
    def extract_solutions_from_question(self, question: Dict) -> List[Dict]:
        """
        Extract all solutions from a question payload
        
        Returns list of solution dicts with:
          - solution_type: 'description_step' | 'question_video' | 'result_video'
          - step_number: Sequential order
          - images: List of image URLs
          - videos: List of video objects {url, type, provider}
          - description_json: Raw description data
        """
        solutions = []
        step_counter = 0
        
        # 1. Extract description steps with images and videos
        descriptions = question.get("solutionDescription") or []
        for desc_idx, desc in enumerate(descriptions):
            if not isinstance(desc, dict):
                continue
            
            solution = {
                "solution_type": "description_step",
                "step_number": step_counter,
                "description_idx": desc_idx,
                "images": self._extract_images_from_description(desc),
                "videos": self._extract_videos_from_description(desc),
                "description_json": json.dumps(desc),
                "text": self._extract_text_from_description(desc),
            }
            solutions.append(solution)
            step_counter += 1
        
        # 2. Extract question-level videos
        question_videos = self._extract_video_urls_from_node(question)
        # Filter out videos that are in descriptions (avoid duplicates)
        description_videos = set()
        for sol in solutions:
            description_videos.update(v.get("url") for v in sol["videos"] if v.get("url"))
        
        for vid_idx, video_url in enumerate(sorted(question_videos)):
            if video_url not in description_videos:
                solution = {
                    "solution_type": "question_video",
                    "step_number": step_counter,
                    "video_idx": vid_idx,
                    "videos": [self._parse_video_url(video_url)],
                    "images": [],
                    "description_json": None,
                    "text": None,
                }
                solutions.append(solution)
                step_counter += 1
        
        return solutions
    
    def extract_solutions_from_result(self, result_question: Dict) -> List[Dict]:
        """Extract solutions from my-result payload"""
        solutions = []
        
        if not isinstance(result_question, dict):
            return solutions
        
        # Result payload may have additional videos/images not in start-test
        result_videos = self._extract_video_urls_from_node(result_question)
        
        for vid_idx, video_url in enumerate(sorted(result_videos)):
            solution = {
                "solution_type": "result_video",
                "step_number": None,  # Result solutions are supplementary
                "video_idx": vid_idx,
                "videos": [self._parse_video_url(video_url)],
                "images": [],
                "description_json": None,
                "text": None,
            }
            solutions.append(solution)
        
        return solutions
    
    # ========================================================================
    # MEDIA EXTRACTION HELPERS
    # ========================================================================
    
    def _extract_images_from_description(self, desc: Dict) -> List[str]:
        """Extract image URLs from description object"""
        images = []
        imageIds = desc.get("imageIds")
        if isinstance(imageIds, dict):
            # Could be nested by language
            for key, value in imageIds.items():
                if isinstance(value, dict):
                    url = self._image_object_to_url(value)
                    if url:
                        images.append(url)
                elif isinstance(value, list):
                    for v in value:
                        if isinstance(v, dict):
                            url = self._image_object_to_url(v)
                            if url:
                                images.append(url)
        elif isinstance(imageIds, list):
            for img in imageIds:
                if isinstance(img, dict):
                    url = self._image_object_to_url(img)
                    if url:
                        images.append(url)
        
        return list(dict.fromkeys(images))  # Deduplicate preserving order
    
    def _extract_videos_from_description(self, desc: Dict) -> List[Dict]:
        """Extract video objects from description"""
        videos = []
        
        # Direct video URL fields
        for vid_field in ["videoSrc", "videoUrl", "solutionVideoUrl"]:
            url = desc.get(vid_field)
            if url:
                videos.append(self._parse_video_url(url))
        
        # Nested video data
        video_data = desc.get("video")
        if isinstance(video_data, dict):
            url = video_data.get("url") or video_data.get("src")
            if url:
                videos.append(self._parse_video_url(url))
        
        # Collect from all fields recursively
        nested_videos = self._extract_video_urls_from_node(desc)
        for url in nested_videos:
            seen_urls = {v.get("url") for v in videos}
            if url not in seen_urls:
                videos.append(self._parse_video_url(url))
        
        return videos
    
    def _extract_text_from_description(self, desc: Dict) -> Optional[str]:
        """Extract description text content"""
        text = desc.get("text") or desc.get("description")
        if isinstance(text, str):
            return text.strip() or None
        return None
    
    def _extract_video_urls_from_node(self, node: Any, found: Optional[set] = None) -> set:
        """Recursively extract all video URLs from a node"""
        if found is None:
            found = set()
        
        if isinstance(node, dict):
            for key, value in node.items():
                lk = str(key).lower()
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    # Check for video indicators
                    if any(x in lk for x in ["video", "solution", "url"]) or \
                       any(x in value.lower() for x in [".mp4", ".mov", ".avi", "youtube", "cloudfront"]):
                        found.add(value)
                else:
                    self._extract_video_urls_from_node(value, found)
        elif isinstance(node, (list, tuple)):
            for item in node:
                self._extract_video_urls_from_node(item, found)
        
        return found
    
    def _image_object_to_url(self, image_obj: Dict) -> Optional[str]:
        """Convert image object to downloadable URL"""
        if not isinstance(image_obj, dict):
            return None
        
        # Direct URL fields
        for url_field in ["url", "src", "imageUrl"]:
            url = image_obj.get(url_field)
            if isinstance(url, str) and url.startswith(("http://", "https://")):
                return url
        
        # Construct from baseUrl + key
        base_url = image_obj.get("baseUrl") or ""
        key = image_obj.get("key")
        if isinstance(key, str) and key.startswith(("http://", "https://")):
            return key
        if base_url and key:
            return f"{base_url.rstrip('/')}/{str(key).lstrip('/')}"
        
        return None
    
    def _parse_video_url(self, url: str) -> Dict[str, Any]:
        """Parse video URL and identify type"""
        video_info = {
            "url": url,
            "type": "unknown",
            "provider": "unknown",
            "is_downloadable": False,
        }
        
        if not isinstance(url, str):
            return video_info
        
        url_lower = url.lower()
        
        # YouTube
        if "youtube.com" in url_lower or "youtu.be" in url_lower:
            video_info["type"] = "youtube"
            video_info["provider"] = "youtube"
            video_info["is_downloadable"] = False  # Reference only
            # Extract YouTube ID
            import re
            yt_patterns = [
                r"youtube\.com/watch\?v=([A-Za-z0-9_-]{8,20})",
                r"youtu\.be/([A-Za-z0-9_-]{8,20})",
            ]
            for pattern in yt_patterns:
                match = re.search(pattern, url)
                if match:
                    video_info["youtube_id"] = match.group(1)
                    break
        
        # CloudFront CDN
        elif "cloudfront.net" in url_lower:
            video_info["type"] = "cloudfront"
            video_info["provider"] = "cloudfront"
            video_info["is_downloadable"] = self._is_cloudfront_downloadable(url)
        
        # Direct download URL (ends with video extension)
        elif any(url_lower.endswith(ext) for ext in [".mp4", ".mov", ".avi", ".mkv", ".webm"]):
            video_info["type"] = "direct"
            video_info["provider"] = "direct"
            video_info["is_downloadable"] = True
        
        # Default to direct/downloadable if it's a full URL
        else:
            video_info["is_downloadable"] = True
        
        return video_info
    
    def _is_cloudfront_downloadable(self, url: str) -> bool:
        """
        Check if a CloudFront URL is directly downloadable
        Root objects (empty path) typically aren't, but files with paths are
        """
        parsed = urlparse(url)
        path = parsed.path
        
        # Root domain or just domain+slash = not downloadable (403 error)
        if not path or path == "/":
            return False
        
        # Path-based CloudFront URLs are typically downloadable
        return True
    
    # ========================================================================
    # SOLUTION STORAGE (DATABASE)
    # ========================================================================
    
    def store_solution(
        self,
        batch_id: str,
        test_id: str,
        question_id: str,
        solution: Dict,
        headers: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Store a single solution in the database
        
        Returns dict with:
          - solution_id: Database record ID
          - assets: List of stored asset IDs
          - errors: List of any errors during storage
        """
        if headers is None:
            headers = {}
        
        solution_type = solution.get("solution_type")
        step_number = solution.get("step_number")
        
        # Create solution record in test_solutions table
        solution_id = self.db_logger.upsert_test_solution(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            solution_type=solution_type,
            step_number=step_number,
            description_json=solution.get("description_json"),
            text=solution.get("text"),
        )
        
        result = {
            "solution_id": solution_id,
            "assets": [],
            "errors": [],
        }
        
        # Store images
        for img_idx, image_url in enumerate(solution.get("images") or []):
            try:
                asset_id = self._store_solution_asset(
                    batch_id=batch_id,
                    test_id=test_id,
                    question_id=question_id,
                    solution_id=solution_id,
                    asset_type="image",
                    source_url=image_url,
                    step_idx=img_idx,
                    headers=headers,
                )
                if asset_id:
                    result["assets"].append(asset_id)
            except Exception as exc:
                result["errors"].append(f"Image {img_idx}: {str(exc)}")
        
        # Store videos
        for video in solution.get("videos") or []:
            try:
                asset_id = self._store_solution_asset(
                    batch_id=batch_id,
                    test_id=test_id,
                    question_id=question_id,
                    solution_id=solution_id,
                    asset_type="video",
                    video_info=video,
                    headers=headers,
                )
                if asset_id:
                    result["assets"].append(asset_id)
            except Exception as exc:
                result["errors"].append(f"Video {video.get('url', 'unknown')}: {str(exc)}")
        
        return result
    
    def _store_solution_asset(
        self,
        batch_id: str,
        test_id: str,
        question_id: str,
        solution_id: int,
        asset_type: str,  # 'image' or 'video'
        source_url: Optional[str] = None,
        video_info: Optional[Dict] = None,
        step_idx: int = 0,
        headers: Optional[Dict] = None,
    ) -> Optional[int]:
        """Store asset (image or video) and return asset ID"""
        
        if asset_type == "image" and source_url:
            return self._store_solution_image(
                batch_id, test_id, question_id, solution_id,
                source_url, step_idx, headers
            )
        
        elif asset_type == "video" and video_info:
            return self._store_solution_video(
                batch_id, test_id, question_id, solution_id,
                video_info, headers
            )
        
        return None
    
    def _store_solution_image(
        self,
        batch_id: str,
        test_id: str,
        question_id: str,
        solution_id: int,
        image_url: str,
        step_idx: int,
        headers: Optional[Dict] = None,
    ) -> Optional[int]:
        """Download image and store to IA"""
        
        if not headers:
            headers = self.batch_api.DEFAULT_HEADERS if self.batch_api else {}
        
        # Check if already stored
        existing = self.db_logger.get_test_asset_by_source(
            batch_id, test_id, "solution_image", image_url
        )
        if existing:
            return existing.get("id")
        
        # Download image
        data, mime_type = self._download_file_bytes(image_url, headers=headers)
        if not data:
            return None
        
        ext = self._get_extension_from_mime(mime_type, ".png")
        
        # Upload to IA
        ia_result = self._upload_bytes_to_ia(
            data, filename_hint=f"solution-img-{step_idx}", ext=ext
        )
        
        if not ia_result.get("ok"):
            return None
        
        # Store in database
        self.db_logger.upsert_test_asset(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            asset_kind="solution_image",
            source_key=image_url,
            source_url=image_url,
            asset_type="image",
            storage_provider="internet_archive",
            storage_id=ia_result.get("identifier"),
            storage_url=ia_result.get("file_url"),
            ia_identifier=ia_result.get("identifier"),
            ia_url=ia_result.get("url"),
            status="done",
        )
        
        return True
    
    def _store_solution_video(
        self,
        batch_id: str,
        test_id: str,
        question_id: str,
        solution_id: int,
        video_info: Dict,
        headers: Optional[Dict] = None,
    ) -> Optional[int]:
        """Download and store solution video"""
        
        if not headers:
            headers = self.batch_api.DEFAULT_HEADERS if self.batch_api else {}
        
        url = video_info.get("url")
        provider = video_info.get("provider")
        is_downloadable = video_info.get("is_downloadable")
        
        # YouTube: store reference only
        if provider == "youtube" and video_info.get("youtube_id"):
            self.db_logger.upsert_test_asset(
                batch_id=batch_id,
                test_id=test_id,
                question_id=question_id,
                asset_kind="solution_video",
                source_key=url,
                source_url=url,
                asset_type="video",
                storage_provider="youtube",
                storage_id=video_info.get("youtube_id"),
                storage_url=f"https://www.youtube.com/watch?v={video_info.get('youtube_id')}",
                youtube_id=video_info.get("youtube_id"),
                status="done",
            )
            return True
        
        # Check if already stored
        existing = self.db_logger.get_test_asset_by_source(
            batch_id, test_id, "solution_video", url
        )
        if existing:
            return existing.get("id")
        
        # Download video
        if not is_downloadable:
            # Try to get signed URL or alternative download link
            pass  # Could implement CloudFront signed URL logic here
        
        data, mime_type = self._download_file_bytes(url, headers=headers)
        if not data:
            self.db_logger.upsert_test_asset(
                batch_id=batch_id,
                test_id=test_id,
                question_id=question_id,
                asset_kind="solution_video",
                source_key=url,
                source_url=url,
                asset_type="video",
                status="failed",
                error="download_failed",
            )
            return None
        
        ext = self._get_extension_from_mime(mime_type, ".mp4")
        
        # Upload to IA
        ia_result = self._upload_bytes_to_ia(
            data, filename_hint="solution-video", ext=ext, media_type="video"
        )
        
        if not ia_result.get("ok"):
            self.db_logger.upsert_test_asset(
                batch_id=batch_id,
                test_id=test_id,
                question_id=question_id,
                asset_kind="solution_video",
                source_key=url,
                source_url=url,
                asset_type="video",
                status="failed",
                error=ia_result.get("error"),
            )
            return None
        
        # Store in database
        self.db_logger.upsert_test_asset(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            asset_kind="solution_video",
            source_key=url,
            source_url=url,
            asset_type="video",
            storage_provider="internet_archive",
            storage_id=ia_result.get("identifier"),
            storage_url=ia_result.get("file_url"),
            ia_identifier=ia_result.get("identifier"),
            ia_url=ia_result.get("url"),
            status="done",
        )
        
        return True
    
    # ========================================================================
    # FILE OPERATIONS
    # ========================================================================
    
    def _download_file_bytes(
        self,
        url: str,
        headers: Optional[Dict] = None,
        timeout: int = 60,
    ) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Download file bytes from URL, return (bytes, mime_type)
        """
        try:
            response = requests.get(url, headers=headers or {}, timeout=timeout, verify=True)
            if response.status_code in (200, 206):
                mime_type = response.headers.get("content-type", "application/octet-stream")
                return response.content, mime_type
        except Exception:
            pass
        
        return None, None
    
    def _upload_bytes_to_ia(
        self,
        content: bytes,
        filename_hint: str,
        ext: str,
        media_type: str = "image",
    ) -> Dict[str, Any]:
        """Upload bytes to Internet Archive"""
        
        try:
            from mainLogic.utils.internet_archive_uploader import upload_file
        except ImportError:
            return {"ok": False, "error": "IA uploader not available"}
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        
        try:
            # Create IA identifier
            identifier = f"{self.ia_prefix}-{filename_hint}"
            
            # Upload
            ident_result = upload_file(
                file_path=tmp_path,
                identifier=identifier,
                title=filename_hint,
            )
            
            file_name = os.path.basename(tmp_path)
            return {
                "ok": True,
                "identifier": ident_result,
                "url": f"https://archive.org/details/{ident_result}",
                "file_url": f"https://archive.org/download/{ident_result}/{file_name}",
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        finally:
            try:
                os.remove(tmp_path)
            except:
                pass
    
    def _get_extension_from_mime(self, mime_type: str, fallback: str = ".bin") -> str:
        """Get file extension from MIME type"""
        if not mime_type:
            return fallback
        
        # Clean up mime type (remove parameters like charset)
        mime = str(mime_type).split(";")[0].strip()
        
        ext = mimetypes.guess_extension(mime)
        return ext if ext else fallback
