#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to clean cache and temporary files supporting Makefile, cross-platform.
"""

import io
import os
import shutil
import sys

# Setup UTF-8 encoding for stdout/stderr on Windows to avoid potential encoding issues
if sys.platform.startswith("win"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


def clean_project():
    # Cache and build directories to delete from the root
    dirs_to_delete = [
        ".pytest_cache",
        ".ruff_cache",
        "build",
        "dist",
    ]

    print("Cleaning up cache and temporary files...")

    for d in dirs_to_delete:
        if os.path.exists(d):
            try:
                shutil.rmtree(d, ignore_errors=True)
                print(f"Deleted directory: {d}")
            except Exception as e:
                print(f"Could not delete directory {d}: {e}")

    # Recursively find and delete __pycache__ and *.egg-info
    deleted_count = 0
    for root, dirs, files in os.walk("."):
        # Skip directories like .git and .venv to avoid scanning unnecessarily
        if ".git" in dirs:
            dirs.remove(".git")
        if ".venv" in dirs:
            dirs.remove(".venv")

        for d in dirs:
            if d == "__pycache__" or d.endswith(".egg-info"):
                path = os.path.join(root, d)
                try:
                    shutil.rmtree(path, ignore_errors=True)
                    print(f"Deleted: {path}")
                    deleted_count += 1
                except Exception as e:
                    print(f"Could not delete {path}: {e}")

    print("Cleanup complete.")


if __name__ == "__main__":
    clean_project()
