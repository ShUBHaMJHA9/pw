import os
import subprocess
import uuid


def identifier_dash(text: str) -> str:
    """
    Convert text to valid Internet Archive identifier.
    Only lowercase letters, numbers and dashes allowed.
    """
    text = (text or "").strip().lower()
    text = text.replace(" ", "-")
    text = "".join(c for c in text if c.isalnum() or c == "-")
    return text[:80] if text else "item"


def upload_file(file_path: str, identifier: str = None) -> str:
    """
    Upload file to Internet Archive using CLI.
    No collection.
    No restricted metadata.
    Safe for normal accounts.
    """

    if not os.path.isfile(file_path):
        raise FileNotFoundError("File not found.")

    # Generate unique identifier if not provided
    if not identifier:
        base = identifier_dash(os.path.splitext(os.path.basename(file_path))[0])
        identifier = f"{base}-{uuid.uuid4().hex[:6]}"

    cmd = [
        "ia", "upload", identifier, file_path,
        "--metadata=mediatype:movies",
        "--no-derive",
        "--retries=3",
        "--checksum"
    ]

    process = subprocess.run(cmd, capture_output=True, text=True)

    if process.returncode != 0:
        print("Upload Error Output:")
        print(process.stderr)
        raise RuntimeError("Internet Archive upload failed")

    print("Upload successful.")
    return identifier
