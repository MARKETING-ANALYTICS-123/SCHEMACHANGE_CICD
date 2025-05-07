# deploy_sql_files_gitdiff.py
import os, json, shutil, time
from datetime import datetime
import snowflake.connector

TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
ARCHIVE_DIR = "./archive"
DAYS = 7
CHANGED_FILES_LIST = 'changed_files.txt'

# Read changed files from git diff output
if os.path.exists(CHANGED_FILES_LIST):
    with open(CHANGED_FILES_LIST, 'r') as f:
        changed_files = [line.strip() for line in f if line.strip().endswith('.sql')]
else:
    print(f"❌ {CHANGED_FILES_LIST} not found.")
    changed_files = []

if not changed_files:
    print("✅ No changed SQL files to deploy.")
    exit(0)


def archive_old_version(file_path, old_content):
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    base_name = os.path.basename(file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_file = os.path.join(ARCHIVE_DIR, f"{base_name}_{timestamp}.sql")
    with open(archive_file, 'w') as f:
        f.write(old_content)
    print(f"🗄️ Archived: {archive_file}")

def clean_old_archives():
    now = time.time()
    if not os.path.exists(ARCHIVE_DIR): return
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

# Deploy changed files
for file in changed_files:
    if not os.path.exists(file):
        print(f"⚠️ Skipping missing file: {file}")
        continue

    schema = 'RPT' if file.startswith(TABLES_FOLDER) else 'XFRM'
    with open(file, 'r') as f:
        content = f.read()

    try:
        print(f"Executing in {schema} schema: {file}")
        cursor.execute(f"USE SCHEMA {schema};")
        cursor.execute(content)
        print(f"✅ Executed: {file}")
    except Exception as e:
        print(f"❌ Failed to execute {file}: {e}")
        continue

cursor.close()
conn.close()
clean_old_archives()
print("🎉 Deployment complete.")
