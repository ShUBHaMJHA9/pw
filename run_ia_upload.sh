#!/usr/bin/env bash
set -euo pipefail

# Target file to upload
f='/workspaces/pw/compilerdesign_CH_01__Introduction_and_Lexical_Analysis_Introduction_and_Lexical_Analysis_05__Lexical_Analysis_Part_03.mp4'
base=$(basename "$f" .mp4)

# create a safe identifier (keep letters/numbers, replace others with -)
id=$(echo "$base" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/-\{2,\}/-/g' | sed 's/^-\|-$//g' | cut -c1-100)

mkdir -p /workspaces/pw/logs
logfile=/workspaces/pw/logs/ia_upload.log
echo "Starting IA upload at: $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$logfile"

# Fallback if identifier is empty
if [ -z "$id" ] || [ "$id" = "-" ]; then
	id="pw-$(date -u +%Y%m%dT%H%M%SZ)"
	echo "Generated fallback identifier: $id" >> "$logfile"
fi

echo "Identifier: $id" >> "$logfile"
echo "File: $f" >> "$logfile"

# Run upload (retries used by ia). Omit flags that may not be supported.
echo "Running: ia upload $id <file>" >> "$logfile"
ia upload "$id" "$f" --metadata mediatype:movies --metadata title:"$base" --retries=5 >> "$logfile" 2>&1

echo "Finished IA upload at: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$logfile"
