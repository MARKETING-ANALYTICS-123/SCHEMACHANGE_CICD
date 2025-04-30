import os
import hashlib
import json
import shutil
import time
from datetime import datetime
import snowflake.connector

# Folder paths
TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
HASH_TRACKER_FILE = '.deployed_hashes.json'
ARCHIVE_DIR = "./archive"
DAYS = 7

# Load previous hash history
if os.path.exists(HASH_TRACKER_FILE):
    with open(HASH_TRACKER_FILE, 'r') as f:
        file_hashes = json.load(f)
else:
    file_hashes = {}

# File hash generator
def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

# Archive function
def archive_old_version(original_path):
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)

    if os.path.isfile(original_path):
        base_name = os.path.basename(original_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{base_name.rsplit('.',1)[0]}_{timestamp}.sql"
        archive_path = os.path.join(ARCHIVE_DIR, archive_name)
        shutil.copy2(original_path, archive_path)
        print(f"📦 Archived: {original_path} -> {archive_path}")

# Cleanup old archives
def clean_old_archives():
    if not os.path.exists(ARCHIVE_DIR):
        return
    now = time.time()
    for file in os.listdir(ARCHIVE_DIR):
        path = os.path.join(ARCHIVE_DIR, file)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > DAYS * 86400:
            os.remove(path)
            print(f"🧹 Removed old archive: {file}")

# Run SQL via Snowflake
def run_sql(cursor, path, schema):
    with open(path, 'r') as f:
        sql = f.read()
    print(f"🔁 Executing {os.path.basename(path)} in schema {schema}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute(sql)

# Snowflake connect
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

# Handle Tables
for file_name in sorted(os.listdir(TABLES_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(TABLES_FOLDER, file_name)
        file_key = f"Tables/{file_name}"
        new_hash = get_file_hash(full_path)

        if file_hashes.get(file_key) != new_hash:
            if file_key in file_hashes:
                archive_old_version(full_path)
            run_sql(cursor, full_path, 'RPT')
            file_hashes[file_key] = new_hash
            print(f"✅ Updated: {file_name}")
        else:
            print(f"⏭️ No changes in: {file_name}")

# Handle Stored Procs
for file_name in sorted(os.listdir(SP_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(SP_FOLDER, file_name)
        file_key = f"StoredProcs/{file_name}"
        new_hash = get_file_hash(full_path)

        if file_hashes.get(file_key) != new_hash:
            if file_key in file_hashes:
                archive_old_version(full_path)
            run_sql(cursor, full_path, 'XFRM')
            file_hashes[file_key] = new_hash
            print(f"✅ Updated: {file_name}")
        else:
            print(f"⏭️ No changes in: {file_name}")

# Save updated hashes
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(file_hashes, f, indent=2)

# Close DB connection
cursor.close()
conn.close()
print("🎉 Deployment finished.")

# Clean up
clean_old_archives()
