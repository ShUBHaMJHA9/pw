#!/usr/bin/env python3
import os
import subprocess
import sys
import shutil

# Access environment variables passed by the GitUpdater
operation = os.getenv("GIT_UPDATER_OPERATION")
prev_commit = os.getenv("GIT_UPDATER_PREV_COMMIT")
curr_commit = os.getenv("GIT_UPDATER_CURR_COMMIT")
repo_path = os.getenv("GIT_UPDATER_REPO_PATH")

print(f"--- Running post-operation script ---")
print(f"Operation: {operation}")
print(f"Previous Commit: {prev_commit}")
print(f"Current Commit: {curr_commit}")
print(f"Repository Path: {repo_path}")

if operation == "update":
    print("Running 'pip install -r requirements.txt' to update dependencies...")
    try:
        # Run pip install from the repository directory
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
                       cwd=repo_path, check=True)
        try:
            pref_path = os.path.join(repo_path, "preferences.json")
            if not os.path.exists(pref_path):
                defaults_linux = os.path.join(repo_path, "defaults.linux.json")
                defaults = os.path.join(repo_path, "defaults.json")
                seed_path = defaults_linux if os.path.exists(defaults_linux) else defaults
                if os.path.exists(seed_path):
                    shutil.copyfile(seed_path, pref_path)
                    print(f"Seeded preferences.json from {os.path.basename(seed_path)}")
        except Exception as e:
            print(f"Failed to seed preferences.json: {e}")
        print("Dependencies updated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error updating dependencies: {e}", file=sys.stderr)
        sys.exit(1) # Indicate failure
elif operation == "rollback":
    print("Handling rollback specific tasks (e.g., reverting migrations)...")
    # Add your rollback-specific logic here
elif operation == "goto":
    print("Handling 'goto' specific tasks...")
    # Add your go-to-version specific logic here

print("--- Post-operation script finished ---")
