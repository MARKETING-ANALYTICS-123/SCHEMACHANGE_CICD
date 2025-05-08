import os
import time
import shutil
from datetime import datetime

import subprocess

import shutil

import snowflake.connector

# Paths and settings
TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
ARCHIVE_DIR = "./archive"
DAYS = 30

# Detect changed files using git diff
print("🔍 Detecting changed SQL files...")
result = subprocess.run(['git', 'diff', '--name-only', 'origin/PROD...HEAD'], capture_output=True, text=True)
changed_files = [f.strip() for f in result.stdout.split('\n') if f.strip().endswith('.sql')]

if not changed_files:
    print("✅ No changed SQL files to process.")
    exit(0)

def archive_old_file(file_path):
    if not os.path.exists(file_path):
        print(f"⚠️ File not found for archiving: {file_path}")
        return

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    base_name = os.path.basename(file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_file = os.path.join(ARCHIVE_DIR, f"{base_name}_{timestamp}.sql")


    shutil.copy2(file_path, archive_file)
    print(f"🗄️ Archived: {archive_file}")


def clean_old_archives():
    """Remove archived files older than the configured retention period."""
    now = time.time()
    if not os.path.exists(ARCHIVE_DIR):
        return
    for file in os.listdir(ARCHIVE_DIR):
        path = os.path.join(ARCHIVE_DIR, file)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > DAYS * 86400:
            os.remove(path)
            print(f"🧹 Deleted old archive: {file}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema='PUBLIC'
)
cursor = conn.cursor()

# Process each changed file
for file in changed_files:
    if not os.path.exists(file):
        print(f"⚠️ Skipping missing file: {file}")
        continue


    # Archive the old file

    archive_old_file(file)

    schema = 'RPT' if TABLES_FOLDER in file else 'XFRM'
    with open(file, 'r') as f:
        content = f.read()

    try:
        print(f"🚀 Deploying to schema {schema}: {file}")
        cursor.execute(f"USE SCHEMA {schema};")
        cursor.execute(content)
        print(f"✅ Successfully deployed: {file}")
    except Exception as e:
        print(f"❌ Deployment failed for {file}: {e}")

cursor.close()
conn.close()
clean_old_archives()
print("🎉 Deployment complete.")
