import os
import time
import shutil
from datetime import datetime
import subprocess
import snowflake.connector

# --- Configuration ---
TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
ARCHIVE_DIR = "./archive"
RETENTION_DAYS = 7

# --- Git: Fetch and diff ---
print("🔍 Fetching and detecting changed SQL files...")
subprocess.run(['git', 'fetch', 'origin'], check=True)

result = subprocess.run(
    ['git', 'diff', '--name-only', 'origin/PROD...HEAD', '--', '*.sql'],
    capture_output=True, text=True
)
changed_files = [f.strip() for f in result.stdout.split('\n') if f.strip()]

if not changed_files:
    print("✅ No changed SQL files to process.")
    exit(0)

print(f"📄 Changed files:\n{chr(10).join(changed_files)}")

# --- Archiving helper ---
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

# --- Clean old archives ---
def clean_old_archives():
    now = time.time()
    if not os.path.exists(ARCHIVE_DIR):
        return
    for file in os.listdir(ARCHIVE_DIR):
        path = os.path.join(ARCHIVE_DIR, file)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > RETENTION_DAYS * 86400:
            os.remove(path)
            print(f"🧹 Deleted old archive: {file}")

# --- Snowflake connection ---
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

# --- Deploy changed SQL files ---
for file in changed_files:
    full_path = os.path.join(os.getcwd(), file)

    if not os.path.exists(full_path):
        print(f"⚠️ Skipping missing file: {full_path}")
        continue

    archive_old_file(full_path)

    schema = 'RPT' if TABLES_FOLDER in file else 'XFRM'
    with open(full_path, 'r') as f:
        content = f.read()

    try:
        print(f"🚀 Deploying to schema {schema}: {file}")
        cursor.execute(f"USE SCHEMA {schema};")
        cursor.execute(content)
        print(f"✅ Successfully deployed: {file}")
    except Exception as e:
        print(f"❌ Deployment failed for {file}: {e}")

# --- Cleanup ---
cursor.close()
conn.close()
clean_old_archives()
print("🎉 Deployment complete.")
