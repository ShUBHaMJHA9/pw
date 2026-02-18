import os
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


def upload_file(file_path: str, identifier: str = None, title: str = None) -> str:
    """
    Upload file to Internet Archive using CLI with real-time progress.
    Shows upload progress, speed, and errors.
    
    Args:
        file_path: Path to file to upload
        identifier: IA identifier (generated if not provided)
        title: Title metadata (uses filename if not provided)
    
    Returns:
        identifier: The Internet Archive identifier
    """

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Generate unique identifier if not provided
    if not identifier:
        base = identifier_dash(os.path.splitext(os.path.basename(file_path))[0])
        identifier = f"{base}-{uuid.uuid4().hex[:6]}"

    # Generate title from filename if not provided
    if not title:
        title = os.path.splitext(os.path.basename(file_path))[0]

    print(f"\n[IA UPLOAD] Starting upload to Internet Archive")
    print(f"[IA UPLOAD] File: {os.path.basename(file_path)}")
    print(f"[IA UPLOAD] Size: {os.path.getsize(file_path) / (1024**3):.2f} GB")
    print(f"[IA UPLOAD] Identifier: {identifier}")
    print(f"[IA UPLOAD] Title: {title}")
    print("-" * 80)

    # Optimized command for fastest upload - removed --checksum to avoid pre-calculation delay
    cmd = f'''ia upload {identifier} "{file_path}" --metadata="title:{title}" --metadata="mediatype:movies" --no-derive'''

    # Run with shell=True for real-time progress display (no output capturing)
    process = subprocess.Popen(cmd, shell=True)
    return_code = process.wait()

    print("-" * 80)
    
    if return_code != 0:
        print(f"[IA UPLOAD ERROR] Upload failed with return code: {return_code}")
        print(f"[IA UPLOAD ERROR] Identifier: {identifier}")
        print(f"[IA UPLOAD ERROR] File: {file_path}")
        raise RuntimeError(f"Internet Archive upload failed with return code {return_code}")

    print(f"[IA UPLOAD SUCCESS] Upload completed successfully!")
    print(f"[IA UPLOAD SUCCESS] URL: https://archive.org/details/{identifier}")
    return identifier
