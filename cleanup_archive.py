import os
import time

ARCHIVE_DIR = "./archive"
DAYS = 7
now = time.time()

for file in os.listdir(ARCHIVE_DIR):
    file_path = os.path.join(ARCHIVE_DIR, file)
    if os.path.isfile(file_path):
        age = now - os.path.getmtime(file_path)
        if age > DAYS * 86400:
            os.remove(file_path)
            print(f"Deleted: {file}")
