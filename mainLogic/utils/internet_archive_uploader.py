import os
import re
import subprocess
import uuid
import sys


def identifier_dash(text: str) -> str:
    """
    Convert text to valid Internet Archive identifier.
    Only lowercase letters, numbers and dashes allowed.
    """
    text = (text or "").strip().lower()
    text = text.replace(" ", "-")
    text = "".join(c for c in text if c.isalnum() or c == "-")
    return text[:80] if text else "item"


def upload_file(
    file_path: str,
    identifier: str = None,
    title: str = None,
    log_callback=None,
    progress_callback=None,
) -> str:
    """Upload file to Internet Archive using CLI with streamed logs/progress callbacks."""

    def _log(msg: str):
        if log_callback:
            try:
                log_callback(msg)
                return
            except Exception:
                pass
        print(msg)

    def _progress(percent: int):
        if progress_callback:
            try:
                progress_callback(percent)
            except Exception:
                pass

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if not identifier:
        base = identifier_dash(os.path.splitext(os.path.basename(file_path))[0])
        identifier = f"{base}-{uuid.uuid4().hex[:6]}"

    if not title:
        title = os.path.splitext(os.path.basename(file_path))[0]

    _log("[IA] Starting upload")
    _log(f"[IA] File: {os.path.basename(file_path)}")
    _log(f"[IA] Size: {os.path.getsize(file_path) / (1024**3):.2f} GB")
    _log(f"[IA] Identifier: {identifier}")
    _progress(0)

    cmd = [
        "ia",
        "upload",
        identifier,
        file_path,
        f"--metadata=title:{title}",
        "--metadata=mediatype:movies",
        "--no-derive",
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
    )

    last_pct = 0
    if process.stdout is not None:
        for raw_line in process.stdout:
            line = (raw_line or "").strip()
            if not line:
                continue
            _log(f"[IA] {line}")
            # Best-effort parse of percentage reported by IA CLI output.
            match = re.search(r"(\d{1,3})%", line)
            if match:
                pct = max(0, min(100, int(match.group(1))))
                if pct >= last_pct:
                    last_pct = pct
                    _progress(last_pct)

    return_code = process.wait()

    if return_code != 0:
        _log(f"[IA ERROR] Upload failed with code: {return_code}")
        raise RuntimeError(f"Internet Archive upload failed with return code {return_code}")

    _progress(100)
    _log("[IA] Upload completed")
    _log(f"[IA] URL: https://archive.org/details/{identifier}")
    return identifier
