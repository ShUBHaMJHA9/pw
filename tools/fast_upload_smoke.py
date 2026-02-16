#!/usr/bin/env python3
"""Minimal smoke test for fast.py pipeline."""

import argparse
import os
from pathlib import Path


def _collect_files(files, directory):
    items = []
    if files:
        for f in files:
            p = Path(f)
            if p.exists() and p.is_file():
                items.append(str(p.resolve()))
    if directory:
        d = Path(directory)
        if d.exists() and d.is_dir():
            for p in sorted(d.iterdir()):
                if p.is_file():
                    items.append(str(p.resolve()))
    return items


def main():
    parser = argparse.ArgumentParser(description="Smoke test for fast.py")
    parser.add_argument("--dir", default="", help="Directory of files to inspect")
    parser.add_argument("--files", default="", help="Comma-separated file list")
    parser.add_argument("--check-env", action="store_true", help="Print required env vars")
    args = parser.parse_args()

    if args.check_env:
        keys = ["TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_CHAT_ID"]
        for k in keys:
            print(f"{k}={'set' if os.environ.get(k) else 'missing'}")

    files = []
    if args.files:
        files = [s.strip() for s in args.files.split(",") if s.strip()]
    items = _collect_files(files, args.dir)
    if not items:
        print("No files found. Use --dir or --files.")
        return
    for p in items:
        try:
            size = os.path.getsize(p)
        except Exception:
            size = None
        print(f"{p} size={size}")


if __name__ == "__main__":
    main()
