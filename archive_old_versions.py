import os
import shutil
import time
from datetime import datetime

ARCHIVE_DIR = "./archive"
if not os.path.exists(ARCHIVE_DIR):
    os.makedirs(ARCHIVE_DIR)

# Get all files in root (excluding folders & hidden)
for filename in os.listdir("."):
    if filename in ["archive", ".git", ".github"] or filename.startswith("."):
        continue

    if os.path.isfile(filename):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        archive_name = f"{filename}_{timestamp}"
        shutil.copy2(filename, os.path.join(ARCHIVE_DIR, archive_name))
        print(f"Archived old version of: {filename} -> {archive_name}")
