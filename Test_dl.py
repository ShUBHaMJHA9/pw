#!/usr/bin/env python3
"""Interactive Test Series downloader and normalized DB ingester.

Flow:
1) Select user profile
2) Select purchased batch
3) Select tests (all or specific)
4) Fetch official test payload via start-test endpoint
5) Store test metadata/questions/options/correct answers only (no user response)
6) Upload question/solution images and non-YouTube solution videos to Internet Archive
"""

import json
import mimetypes
import os
import re
import sys
import tempfile
from urllib.parse import urlparse

import requests

from beta.batch_scraper_2.Endpoints import Endpoints
from beta.batch_scraper_2.module import ScraperModule
from mainLogic.utils.Endpoint import Endpoint
from mainLogic.utils.glv_var import PREFS_FILE, debugger

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def _safe_filename(name, default="item"):
    text = str(name or "").strip()
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return text or default


def _pick_from_list(items, title, get_label):
    if not items:
        return []
    print(title)
    for idx, item in enumerate(items, start=1):
        print(f"  {idx}. {get_label(item)}")
    choice = input("Enter number(s) comma-separated, or 'all' to pick all: ").strip()
    if not choice or choice.lower() == "all":
        return items
    selected = []
    for part in choice.split(","):
        try:
            i = int(part.strip()) - 1
            if 0 <= i < len(items):
                selected.append(items[i])
        except ValueError:
            continue
    return selected


def _select_user_and_init_api(prefs):
    users = prefs.get("users", []) if isinstance(prefs, dict) else []
    if not users:
        return ScraperModule.batch_api, None

    print("Multiple user profiles found. Select user to use for API requests:")
    for idx, user in enumerate(users, start=1):
        uname = user.get("name") or user.get("username") or f"user-{idx}"
        token_preview = (user.get("access_token") or user.get("token") or "")[:8]
        print(f"  {idx}. {uname} (token startswith: {token_preview}...)")
    print("  a. Add new user")
    print("  q. Quit")

    sel = input("Choose user (number) or action [1]: ").strip() or "1"
    if sel.lower() == "q":
        print("Exiting.")
        sys.exit(0)

    if sel.lower() == "a":
        name = input("Enter profile name: ").strip() or f"user-{len(users) + 1}"
        token = input("Enter access token (Bearer token string): ").strip()
        random_id = input("Enter random_id (optional): ").strip() or None
        new_user = {"name": name, "access_token": token}
        if random_id:
            new_user["random_id"] = random_id
        users.append(new_user)

        try:
            with open(PREFS_FILE, "r", encoding="utf-8") as handle:
                pf = json.load(handle)
        except Exception:
            pf = prefs if isinstance(prefs, dict) else {}

        pf["users"] = users
        try:
            with open(PREFS_FILE, "w", encoding="utf-8") as handle:
                json.dump(pf, handle, indent=2)
            print(f"Saved new profile to preferences: {PREFS_FILE}")
        except Exception as exc:
            debugger.error(f"Failed to save preferences file: {exc}")

        chosen = users[-1]
    else:
        try:
            idx = int(sel) - 1
            if idx < 0 or idx >= len(users):
                idx = 0
            chosen = users[idx]
        except ValueError:
            chosen = users[0]

    token = chosen.get("access_token") or chosen.get("token") or chosen.get("token_config", {}).get("access_token")
    random_id = chosen.get("random_id") or chosen.get("randomId") or chosen.get("token_config", {}).get("random_id")
    if not token:
        debugger.error("Selected profile does not have a token.")
        return ScraperModule.batch_api, None

    try:
        if random_id:
            return Endpoints(verbose=False).set_token(token, random_id=random_id), random_id
        return Endpoints(verbose=False).set_token(token), random_id
    except Exception as exc:
        debugger.error(f"Failed to initialize API with selected profile: {exc}")
        return ScraperModule.batch_api, random_id


def _fetch_tests_for_batch(batch_api, batch_id):
    url = (
        "https://api.penpencil.co/v3/test-service/tests"
        f"?testType=All&testStatus=All&attemptStatus=All&batchId={batch_id}"
        "&isSubjective=false&isPurchased=true"
    )
    payload, status_code, _ = Endpoint(url=url, headers=batch_api.DEFAULT_HEADERS).fetch()
    if status_code != 200 or not isinstance(payload, dict):
        return []
    return payload.get("data") or []


def _start_test(batch_api, test_id, batch_id):
    url = (
        f"https://api.penpencil.co/v3/test-service/tests/{test_id}/start-test"
        f"?testSource=BATCH_TEST_SERIES&type=Start&batchId={batch_id}"
    )
    payload, status_code, _ = Endpoint(url=url, headers=batch_api.DEFAULT_HEADERS).fetch()
    
    # Enhanced validation
    if status_code != 200:
        debugger.error(f"API returned HTTP {status_code}")
        if isinstance(payload, dict):
            err = payload.get("error", {})
            msg = err.get("message") if isinstance(err, dict) else str(err)
            debugger.error(f"Error: {msg}")
        return None, url
    
    if not isinstance(payload, dict):
        debugger.error(f"Invalid API response type: {type(payload)}")
        return None, url
    
    if not payload.get("success"):
        debugger.error("API returned success=false")
        return None, url
    
    data = payload.get("data")
    if not isinstance(data, dict):
        debugger.error("Response data is missing or not a dict")
        return None, url
    
    # Check if questions are present
    sections = data.get("sections") or []
    total_questions = sum(len(s.get("questions") or []) for s in sections)
    
    if not sections or total_questions == 0:
        debugger.warning(f"Test response has no questions (sections={len(sections)}, questions={total_questions})")
        return payload, url  # Still return payload in case partial data
    
    debugger.info(f"✓ API Response valid: {len(sections)} sections, {total_questions} questions")
    return payload, url


def _image_object_to_url(image_obj):
    if not isinstance(image_obj, dict):
        return None
    base_url = image_obj.get("baseUrl") or ""
    key = image_obj.get("key")
    if isinstance(key, str) and key.startswith(("http://", "https://")):
        return key
    if base_url and key:
        return f"{base_url.rstrip('/')}/{str(key).lstrip('/')}"
    return None


def _extract_image_payload(image_ids, language_code="en"):
    if not isinstance(image_ids, dict):
        return None
    if language_code in image_ids and isinstance(image_ids[language_code], dict):
        return image_ids[language_code]
    for value in image_ids.values():
        if isinstance(value, dict):
            return value
    return None


def _extract_image_payloads(image_ids, language_code="en"):
    """Return a list of normalized image payload dicts for all detected images."""
    payloads = []
    if isinstance(image_ids, dict):
        preferred = image_ids.get(language_code)
        if isinstance(preferred, list):
            payloads.extend([p for p in preferred if isinstance(p, dict)])
        elif isinstance(preferred, dict):
            payloads.append(preferred)

        if not payloads:
            for value in image_ids.values():
                if isinstance(value, list):
                    payloads.extend([p for p in value if isinstance(p, dict)])
                elif isinstance(value, dict):
                    payloads.append(value)
                    break
    elif isinstance(image_ids, list):
        payloads.extend([p for p in image_ids if isinstance(p, dict)])

    dedup = []
    seen = set()
    for payload in payloads:
        key = str(payload.get("_id") or payload.get("key") or json.dumps(payload, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(payload)
    return dedup


def _question_id_from_node(question):
    if not isinstance(question, dict):
        return ""
    return str(
        question.get("_id")
        or question.get("id")
        or question.get("questionId")
        or question.get("qid")
        or ""
    )


def _extract_numeric(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                continue
            try:
                if "." in text:
                    return float(text)
                return int(text)
            except ValueError:
                continue
    return None


def _normalize_option_id_list(values):
    ids = []
    seen = set()

    def _push(v):
        text = str(v or "").strip()
        if not text or text in seen:
            return
        seen.add(text)
        ids.append(text)

    if isinstance(values, (str, int, float)):
        _push(values)
        return ids

    if isinstance(values, dict):
        for key in ("_id", "id", "optionId", "option_id", "value"):
            if values.get(key):
                _push(values.get(key))
                break
        return ids

    if isinstance(values, list):
        for item in values:
            if isinstance(item, dict):
                for key in ("_id", "id", "optionId", "option_id", "value"):
                    if item.get(key):
                        _push(item.get(key))
                        break
            else:
                _push(item)
    return ids


def _extract_correct_option_ids(question, result_question=None, result_item=None):
    """Extract correct option ids from all known API shapes."""
    result_question = result_question if isinstance(result_question, dict) else {}
    result_item = result_item if isinstance(result_item, dict) else {}

    candidates = [
        question.get("solutions") if isinstance(question, dict) else None,
        question.get("correctOptions") if isinstance(question, dict) else None,
        question.get("correctOptionIds") if isinstance(question, dict) else None,
        question.get("correctAnswer") if isinstance(question, dict) else None,
        question.get("answerIds") if isinstance(question, dict) else None,
        result_question.get("solutions"),
        result_question.get("correctOptions"),
        result_question.get("correctOptionIds"),
        result_question.get("correctAnswer"),
        result_question.get("answerIds"),
    ]

    topper_result = result_item.get("topperResult") or {}
    if isinstance(topper_result, dict):
        candidates.extend(
            [
                topper_result.get("markedSolutions"),
                topper_result.get("solutions"),
                topper_result.get("correctOptions"),
            ]
        )

    for candidate in candidates:
        ids = _normalize_option_id_list(candidate)
        if ids:
            return ids

    # Final fallback: infer from option-level flags
    options = []
    if isinstance(question, dict):
        options = question.get("options") or []
    if not options and isinstance(result_question, dict):
        options = result_question.get("options") or []

    inferred = []
    for opt in options:
        if not isinstance(opt, dict):
            continue
        if opt.get("isCorrect") is True or opt.get("correct") is True or opt.get("isAnswer") is True:
            opt_id = str(opt.get("_id") or opt.get("id") or opt.get("optionId") or "").strip()
            if opt_id:
                inferred.append(opt_id)
    if inferred:
        return inferred

    return []


def _extract_correct_answer_text(question, result_question=None, result_item=None):
    """Extract text/numeric correct answer when option ids are unavailable."""
    result_question = result_question if isinstance(result_question, dict) else {}
    result_item = result_item if isinstance(result_item, dict) else {}

    candidates = [
        question.get("solutionText") if isinstance(question, dict) else None,
        question.get("correctAnswerText") if isinstance(question, dict) else None,
        question.get("answerText") if isinstance(question, dict) else None,
        question.get("correctAnswer") if isinstance(question, dict) and isinstance(question.get("correctAnswer"), str) else None,
        result_question.get("solutionText"),
        result_question.get("correctAnswerText"),
        result_question.get("answerText"),
        result_question.get("correctAnswer") if isinstance(result_question.get("correctAnswer"), str) else None,
    ]

    topper_result = result_item.get("topperResult") or {}
    your_result = result_item.get("yourResult") or {}
    average_result = result_item.get("averageResult") or {}
    for src in (topper_result, your_result, average_result):
        if isinstance(src, dict):
            candidates.extend(
                [
                    src.get("markedSolutionText"),
                    src.get("solutionText"),
                    src.get("answerText"),
                ]
            )

    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _parse_youtube_id(url):
    if not url:
        return None
    text = str(url).strip()
    patterns = [
        r"youtube\.com/watch\?v=([A-Za-z0-9_-]{8,20})",
        r"youtu\.be/([A-Za-z0-9_-]{8,20})",
        r"youtube\.com/embed/([A-Za-z0-9_-]{8,20})",
    ]
    for pat in patterns:
        match = re.search(pat, text)
        if match:
            return match.group(1)
    return None


def _looks_like_video_url(url):
    if not isinstance(url, str):
        return False
    text = url.strip()
    if not text.startswith(("http://", "https://")):
        return False
    lowered = text.lower()
    parsed = urlparse(text)
    path = (parsed.path or "").strip()
    if path in ("", "/"):
        return False

    video_hints = (
        ".mp4",
        ".m3u8",
        ".mov",
        ".mkv",
        ".webm",
        "manifest",
        "playlist",
        "video",
        "stream",
    )
    if any(token in lowered for token in video_hints):
        return True
    if _parse_youtube_id(text):
        return True
    return False


def _build_url_from_base_key(node):
    if not isinstance(node, dict):
        return None
    base_url = node.get("baseUrl") or node.get("base_url")
    key = node.get("key") or node.get("path") or node.get("file")
    if isinstance(key, str) and key.startswith(("http://", "https://")):
        return key
    if isinstance(base_url, str) and isinstance(key, str) and base_url.strip() and key.strip():
        return f"{base_url.rstrip('/')}/{key.lstrip('/')}"
    return None


def _collect_video_urls(node, found=None):
    if found is None:
        found = set()
    if isinstance(node, dict):
        candidate = _build_url_from_base_key(node)
        if candidate and _looks_like_video_url(candidate):
            found.add(candidate)

        for key, value in node.items():
            lk = str(key).lower()
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                if (
                    "video" in lk
                    or "solutionvideo" in lk
                    or "videosrc" in lk
                    or "videourl" in lk
                    or "url" in lk
                    or "youtube" in lk
                ) and _looks_like_video_url(value):
                    found.add(value)
            else:
                _collect_video_urls(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_video_urls(item, found)
    return found


def _fetch_test_result(batch_api, test_id, test_mapping_id):
    """Fetch my-result payload (when available) to capture solved answer data."""
    if not (test_id and test_mapping_id):
        return None
    url = (
        f"https://api.penpencil.co/v3/test-service/tests/{test_id}/my-result"
        f"?testId={test_id}&testMappingId={test_mapping_id}"
    )
    payload, status_code, _ = Endpoint(url=url, headers=batch_api.DEFAULT_HEADERS).fetch()
    if status_code == 200 and isinstance(payload, dict) and payload.get("success"):
        return payload
    return None


def _index_result_questions(result_payload):
    """Map result question payload by question id for quick fallback lookups."""
    result_map = {}
    if not isinstance(result_payload, dict):
        return result_map
    for item in ((result_payload.get("data") or {}).get("questions") or []):
        if not isinstance(item, dict):
            continue
        q = item.get("question") or {}
        qid = _question_id_from_node(q)
        if qid:
            result_map[qid] = item
    return result_map


def _download_file_bytes(url, headers=None, timeout=45):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200 and r.content:
            return r.content, r.headers.get("content-type")
    except Exception:
        pass

    if headers:
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r.content, r.headers.get("content-type")
        except Exception:
            pass

    return None, None


def _is_cloudfront_root_url(url):
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return False
    if "cloudfront.net" not in (parsed.netloc or ""):
        return False
    path = (parsed.path or "").strip()
    return path in ("", "/")


def _is_video_mime(mime_type):
    if not mime_type:
        return False
    head = str(mime_type).split(";", 1)[0].strip().lower()
    if head.startswith("video/"):
        return True
    return head in {
        "application/octet-stream",
        "application/x-mpegurl",
        "application/vnd.apple.mpegurl",
    }


def _extension_from_mime(mime_type, fallback_ext=".bin"):
    if not mime_type:
        return fallback_ext
    ext = mimetypes.guess_extension(str(mime_type).split(";")[0].strip())
    return ext or fallback_ext


def _upload_to_ia_bytes(content, filename_hint, ext, media_type="image"):
    try:
        from mainLogic.utils.internet_archive_uploader import upload_file, identifier_dash
    except Exception as exc:
        return {"ok": False, "error": f"internet_archive importer failed: {exc}"}

    prefix = os.getenv("IA_IDENTIFIER_PREFIX") or "pw-test-series"
    base = identifier_dash(_safe_filename(filename_hint, default="asset"))
    identifier = identifier_dash(f"{prefix}-{base}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        identifier_result = upload_file(
            file_path=tmp_path,
            identifier=identifier,
            title=filename_hint,
        )
        file_name = os.path.basename(tmp_path)
        return {
            "ok": True,
            "identifier": identifier_result,
            "url": f"https://archive.org/details/{identifier_result}",
            "file_url": f"https://archive.org/download/{identifier_result}/{file_name}",
            "provider": "internet_archive",
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


def _upload_url_asset_to_ia(url, source_key, headers, default_ext, expected_kind=None):
    if expected_kind == "video" and _is_cloudfront_root_url(url):
        return {"ok": False, "error": "cloudfront_root_not_downloadable"}

    data, mime_type = _download_file_bytes(url, headers=headers)
    if not data:
        return {"ok": False, "error": "download_failed"}

    if expected_kind == "video" and not _is_video_mime(mime_type):
        return {"ok": False, "error": f"unexpected_video_mime:{mime_type}"}

    ext = _extension_from_mime(mime_type, fallback_ext=default_ext)
    filename_hint = source_key or os.path.basename(urlparse(url).path) or "asset"
    return _upload_to_ia_bytes(data, filename_hint=filename_hint, ext=ext)


def _store_question_image(db_logger, batch_id, test_id, question_id, source_key, image_url, headers):
    existing = db_logger.get_test_asset_by_source(batch_id, test_id, "question_image", source_key)
    if existing and existing.get("storage_url"):
        return {
            "provider": existing.get("storage_provider"),
            "storage_id": existing.get("storage_id"),
            "storage_url": existing.get("storage_url"),
        }

    upload = _upload_url_asset_to_ia(image_url, source_key, headers=headers, default_ext=".png")
    if not upload.get("ok"):
        return {"provider": None, "storage_id": None, "storage_url": None}

    db_logger.upsert_test_asset(
        batch_id=batch_id,
        test_id=test_id,
        question_id=question_id,
        asset_kind="question_image",
        source_key=source_key,
        source_url=image_url,
        asset_type="image",
        storage_provider=upload.get("provider"),
        storage_id=upload.get("identifier"),
        storage_url=upload.get("file_url") or upload.get("url"),
        status="done",
    )
    return {
        "provider": upload.get("provider"),
        "storage_id": upload.get("identifier"),
        "storage_url": upload.get("file_url") or upload.get("url"),
    }


def _store_solution_image(db_logger, batch_id, test_id, question_id, source_key, image_url, headers):
    existing = db_logger.get_test_asset_by_source(batch_id, test_id, "solution_image", source_key)
    if existing and existing.get("storage_url"):
        return

    upload = _upload_url_asset_to_ia(image_url, source_key, headers=headers, default_ext=".png")
    if not upload.get("ok"):
        return

    db_logger.upsert_test_asset(
        batch_id=batch_id,
        test_id=test_id,
        question_id=question_id,
        asset_kind="solution_image",
        source_key=source_key,
        source_url=image_url,
        asset_type="image",
        storage_provider=upload.get("provider"),
        storage_id=upload.get("identifier"),
        storage_url=upload.get("file_url") or upload.get("url"),
        status="done",
    )


def _store_solution_video(db_logger, batch_id, test_id, question_id, source_key, video_url, headers):
    youtube_id = _parse_youtube_id(video_url)
    if youtube_id:
        db_logger.upsert_test_asset(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            asset_kind="solution_video",
            source_key=source_key,
            source_url=video_url,
            asset_type="video",
            storage_provider="youtube",
            storage_id=youtube_id,
            storage_url=f"https://www.youtube.com/watch?v={youtube_id}",
            youtube_id=youtube_id,
            status="done",
        )
        return

    existing = db_logger.get_test_asset_by_source(batch_id, test_id, "solution_video", source_key)
    if existing and existing.get("storage_url"):
        return

    upload = _upload_url_asset_to_ia(
        video_url,
        source_key,
        headers=headers,
        default_ext=".mp4",
        expected_kind="video",
    )
    if not upload.get("ok"):
        db_logger.upsert_test_asset(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            asset_kind="solution_video",
            source_key=source_key,
            source_url=video_url,
            asset_type="video",
            status="failed",
            error=upload.get("error") or "upload_failed",
        )
        return

    db_logger.upsert_test_asset(
        batch_id=batch_id,
        test_id=test_id,
        question_id=question_id,
        asset_kind="solution_video",
        source_key=source_key,
        source_url=video_url,
        asset_type="video",
        storage_provider=upload.get("provider"),
        storage_id=upload.get("identifier"),
        storage_url=upload.get("file_url") or upload.get("url"),
        status="done",
    )


def _process_test_payload(
    db_logger,
    batch,
    test_summary,
    payload,
    source_url,
    auth_headers,
    result_question_map=None,
):
    data = payload.get("data") or {}
    test_meta = data.get("test") or {}
    test_id = str(test_meta.get("_id") or test_summary.get("_id") or test_summary.get("id") or "")
    if not test_id:
        raise RuntimeError("test_id_missing")

    language_code = "en"
    for row in data.get("languageCodes") or []:
        if row.get("isSelected"):
            language_code = row.get("code") or "en"
            break

    batch_id = str(batch.get("_id") or "")
    db_logger.upsert_test(
        batch_id=batch_id,
        test_id=test_id,
        test_name=test_meta.get("name") or test_summary.get("name"),
        test_type=test_meta.get("type"),
        test_template=test_meta.get("template"),
        source_url=source_url,
        language_code=language_code,
        sections_json=json.dumps(data.get("sections") or []),
        difficulty_levels_json=json.dumps(data.get("difficultyLevels") or []),
        status="downloading",
    )

    # Extract questions from sections (current API structure)
    # API returns: data.sections[].questions[] NOT data.questions[]
    all_questions = []
    sections = data.get("sections") or []
    for section in sections:
        section_questions = section.get("questions") or []
        for q in section_questions:
            all_questions.append(q)
    
    questions = all_questions
    if not questions:
        # Fallback to old format if it ever appears
        questions = data.get("questions") or []
    
    debugger.info(f"Processing {len(questions)} questions from {len(sections)} sections")
    processed_count = 0
    
    for container in questions:
        # Questions from sections are direct dict objects (not wrapped in "question" key)
        question = container if isinstance(container, dict) and "questionNumber" in container else container.get("question")
        if not isinstance(question, dict):
            continue

        question_id = _question_id_from_node(question)
        if not question_id:
            continue
        
        result_item = (result_question_map or {}).get(question_id, {})
        result_question = result_item.get("question") if isinstance(result_item, dict) else {}
        if not isinstance(result_question, dict):
            result_question = {}

        question_number = question.get("questionNumber") or result_question.get("questionNumber")
        question_type = question.get("type") or result_question.get("type")
        positive_marks = _extract_numeric(
            question.get("positiveMarks"),
            question.get("positiveMarksStr"),
            question.get("marks"),
            result_question.get("positiveMarks"),
            result_question.get("positiveMarksStr"),
            result_question.get("marks"),
        )
        negative_marks = _extract_numeric(
            question.get("negativeMarks"),
            question.get("negativeMarksStr"),
            result_question.get("negativeMarks"),
            result_question.get("negativeMarksStr"),
        )
        
        debugger.debug(f"Processing Q{question_number}: ID={question_id}, Type={question_type}, Marks=+{positive_marks}/{negative_marks}")

        # Extract and store all question images
        image_payloads = _extract_image_payloads(question.get("imageIds"), language_code=language_code)
        if not image_payloads and isinstance(result_question, dict):
            image_payloads = _extract_image_payloads(result_question.get("imageIds"), language_code=language_code)

        for image_obj in image_payloads:
            image_url = _image_object_to_url(image_obj)
            if not image_url:
                continue
            source_key = str(image_obj.get("_id") or image_obj.get("key") or image_url)
            debugger.debug(f"  Storing question image: {source_key}")
            _store_question_image(
                db_logger=db_logger,
                batch_id=batch_id,
                test_id=test_id,
                question_id=question_id,
                source_key=source_key,
                image_url=image_url,
                headers=auth_headers,
            )

        # Store question metadata with marks and correct answers
        correct_answers = _extract_correct_option_ids(
            question=question,
            result_question=result_question,
            result_item=result_item,
        )
        correct_answer_text = _extract_correct_answer_text(
            question=question,
            result_question=result_question,
            result_item=result_item,
        )
        debugger.debug(f"  Correct answers: {correct_answers}")
        if not correct_answers and not correct_answer_text:
            debugger.warning(f"  No correct-answer payload from API for question_id={question_id}")
        
        db_logger.upsert_test_question(
            batch_id=batch_id,
            test_id=test_id,
            question_id=question_id,
            question_number=question_number,
            question_type=question_type,
            positive_marks=positive_marks,
            negative_marks=negative_marks,
            difficulty_level=question.get("difficultyLevel") or result_question.get("difficultyLevel"),
            section_id=str(question.get("sectionId") or result_question.get("sectionId") or "") or None,
            subject_id=str(question.get("subjectId") or result_question.get("subjectId") or "") or None,
            chapter_id=str(question.get("chapterId") or result_question.get("chapterId") or "") or None,
            topic_id=(question.get("topicId") or {}).get("_id") if isinstance(question.get("topicId"), dict) else (question.get("topicId") or ((result_question.get("topicId") or {}).get("_id") if isinstance(result_question.get("topicId"), dict) else result_question.get("topicId"))),
            sub_topic_id=str(question.get("subTopicId") or result_question.get("subTopicId") or "") or None,
            qbg_id=question.get("qbgId") or result_question.get("qbgId"),
            qbg_subject_id=question.get("qbgSubjectId") or result_question.get("qbgSubjectId"),
            qbg_chapter_id=question.get("qbgChapterId") or result_question.get("qbgChapterId"),
            qbg_topic_id=question.get("qbgTopicId") or result_question.get("qbgTopicId"),
            correct_option_ids_json=json.dumps(correct_answers),
            correct_answer_text=correct_answer_text,
        )

        # Store all options for this question
        options = question.get("options") or []
        if not options and isinstance(result_question, dict):
            options = result_question.get("options") or []
        debugger.debug(f"  Storing {len(options)} options")
        
        for option in options:
            if not isinstance(option, dict):
                continue
            option_id = str(option.get("_id") or option.get("id") or option.get("optionId") or "")
            if not option_id:
                continue
            texts = option.get("texts") or {}
            option_text = None
            if isinstance(texts, dict):
                option_text = texts.get(language_code)
                if not option_text:
                    for value in texts.values():
                        if isinstance(value, str) and value.strip():
                            option_text = value
                            break
            db_logger.upsert_test_option(
                batch_id=batch_id,
                test_id=test_id,
                question_id=question_id,
                option_id=option_id,
                option_text=option_text,
            )

        solution_descriptions = question.get("solutionDescription") or []
        if not solution_descriptions and isinstance(result_question, dict):
            solution_descriptions = result_question.get("solutionDescription") or []

        for desc in solution_descriptions:
            if not isinstance(desc, dict):
                continue
            desc_image = _extract_image_payload(desc.get("imageIds"), language_code=language_code)
            desc_url = _image_object_to_url(desc_image)
            if desc_url:
                source_key = str(desc_image.get("_id") or desc_image.get("key") or desc_url)
                _store_solution_image(
                    db_logger=db_logger,
                    batch_id=batch_id,
                    test_id=test_id,
                    question_id=question_id,
                    source_key=source_key,
                    image_url=desc_url,
                    headers=auth_headers,
                )

            for idx, video_url in enumerate(sorted(_collect_video_urls(desc))):
                source_key = f"{question_id}:desc_video:{idx}:{_safe_filename(video_url, 'video')}"
                _store_solution_video(
                    db_logger=db_logger,
                    batch_id=batch_id,
                    test_id=test_id,
                    question_id=question_id,
                    source_key=source_key,
                    video_url=video_url,
                    headers=auth_headers,
                )

        for idx, video_url in enumerate(sorted(_collect_video_urls(question))):
            source_key = f"{question_id}:question_video:{idx}:{_safe_filename(video_url, 'video')}"
            _store_solution_video(
                db_logger=db_logger,
                batch_id=batch_id,
                test_id=test_id,
                question_id=question_id,
                source_key=source_key,
                video_url=video_url,
                headers=auth_headers,
            )

        if isinstance(result_question, dict):
            for idx, video_url in enumerate(sorted(_collect_video_urls(result_question))):
                source_key = f"{question_id}:result_video:{idx}:{_safe_filename(video_url, 'video')}"
                _store_solution_video(
                    db_logger=db_logger,
                    batch_id=batch_id,
                    test_id=test_id,
                    question_id=question_id,
                    source_key=source_key,
                    video_url=video_url,
                    headers=auth_headers,
                )

        processed_count += 1

    debugger.info(f"Stored {processed_count} questions for test_id={test_id}")

    db_logger.upsert_test(
        batch_id=batch_id,
        test_id=test_id,
        status="done",
        error=None,
    )


def main():
    prefs = ScraperModule.prefs or {}
    batch_api, _ = _select_user_and_init_api(prefs)
    if batch_api is None:
        debugger.error("Batch API could not be initialized.")
        return

    if not os.getenv("PWDL_DB_URL"):
        debugger.error("PWDL_DB_URL is required for test series storage.")
        return

    try:
        from mainLogic.utils import mysql_logger as db_logger

        db_logger.init(None)
        db_logger.ensure_schema()
    except Exception as exc:
        debugger.error(f"DB logger init failed: {exc}")
        return

    try:
        batches = batch_api.get_purchased_batches(all_pages=True) or []
    except Exception as exc:
        debugger.error(f"Failed to load purchased batches: {exc}")
        return

    if not batches:
        debugger.error("No purchased batches found.")
        return

    selected_batch_list = _pick_from_list(
        batches,
        "Available purchased batches:",
        lambda b: f"{b.get('name') or b.get('slug') or b.get('_id')} (id={b.get('_id')}, slug={b.get('slug')})",
    )
    if not selected_batch_list:
        debugger.error("No batch selected.")
        return

    batch = selected_batch_list[0]
    batch_id = str(batch.get("_id") or "")
    if not batch_id:
        debugger.error("Selected batch does not have an id.")
        return

    tests = _fetch_tests_for_batch(batch_api, batch_id)
    if not tests:
        debugger.error("No tests found for selected batch.")
        return

    selected_tests = _pick_from_list(
        tests,
        f"Tests in batch: {batch.get('name') or batch_id}",
        lambda t: f"{t.get('name') or t.get('_id') or t.get('id')} (testId={t.get('_id') or t.get('id')})",
    )
    if not selected_tests:
        debugger.error("No tests selected.")
        return

    print("\n" + "=" * 100)
    print("STARTING TEST DOWNLOAD & STORAGE")
    print("=" * 100 + "\n")
    
    successful = 0
    failed = 0
    skipped = 0

    for test_summary in selected_tests:
        test_id = str(test_summary.get("_id") or test_summary.get("id") or "")
        test_name = test_summary.get("name") or test_id
        if not test_id:
            debugger.warning("Skipping test with missing id.")
            continue

        existing = db_logger.get_test(batch_id=batch_id, test_id=test_id)
        if existing and existing.get("status") == "done":
            debugger.info(f"⏭️  Skipping already stored test: {test_name} ({test_id})")
            skipped += 1
            continue

        debugger.info(f"\n{'='*80}")
        debugger.info(f"Processing test: {test_name} ({test_id})")
        debugger.info(f"{'='*80}")
        
        payload, source_url = _start_test(batch_api, test_id, batch_id)
        
        if not payload:
            error_text = "API call failed"
            db_logger.upsert_test(
                batch_id=batch_id,
                test_id=test_id,
                test_name=test_name,
                status="failed",
                error=error_text,
            )
            debugger.error(f"❌ Failed to fetch test data: {test_name}")
            failed += 1
            continue
        
        if not isinstance(payload, dict):
            error_text = "Invalid response format"
            db_logger.upsert_test(
                batch_id=batch_id,
                test_id=test_id,
                test_name=test_name,
                status="failed",
                error=error_text,
            )
            debugger.error(f"❌ Invalid API response type")
            failed += 1
            continue
        
        if not payload.get("success"):
            error_text = payload.get("error", {}).get("message", "API error") if isinstance(payload.get("error"), dict) else "Unknown error"
            db_logger.upsert_test(
                batch_id=batch_id,
                test_id=test_id,
                test_name=test_name,
                status="failed",
                error=error_text,
            )
            debugger.error(f"❌ API returned success=false: {error_text}")
            failed += 1
            continue

        try:
            mapping_id = (
                test_summary.get("testStudentMappingId")
                or ((payload.get("data") or {}).get("testStudentMapping") or {}).get("_id")
            )
            result_payload = _fetch_test_result(batch_api, test_id=test_id, test_mapping_id=mapping_id)
            result_map = _index_result_questions(result_payload)

            _process_test_payload(
                db_logger=db_logger,
                batch=batch,
                test_summary=test_summary,
                payload=payload,
                source_url=source_url,
                auth_headers=batch_api.DEFAULT_HEADERS,
                result_question_map=result_map,
            )
            debugger.success(f"✅ Test stored successfully: {test_name}")
            print(f"\n✅ SUCCESS - All questions, options, marks, and solutions stored for: {test_name}\n")
            successful += 1
        except Exception as exc:
            error_msg = str(exc)[:500]
            db_logger.upsert_test(
                batch_id=batch_id,
                test_id=test_id,
                test_name=test_name,
                status="failed",
                error=error_msg,
            )
            debugger.error(f"❌ Failed processing test {test_name}: {exc}")
            import traceback
            debugger.debug(traceback.format_exc())
            failed += 1
    
    # Print summary
    print("\n" + "=" * 100)
    print("DOWNLOAD SUMMARY")
    print("=" * 100)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏭️  Skipped (already stored): {skipped}")
    print(f"📊 Total: {successful + failed + skipped}")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()
