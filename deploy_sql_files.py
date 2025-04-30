import os
import hashlib
import json
import shutil
import time
from datetime import datetime
import snowflake.connector

# Folder paths for tables, stored procedures, and archive directory
TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
HASH_TRACKER_FILE = '.deployed_hashes.json'
ARCHIVE_DIR = "./archive"
DAYS = 7

# Load previous hash + content history
if os.path.exists(HASH_TRACKER_FILE):
    with open(HASH_TRACKER_FILE, 'r') as f:
        file_hashes = json.load(f)
else:
    file_hashes = {}

# Function to calculate file hash
def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        file_data = f.read()
        return hashlib.sha256(file_data).hexdigest()

# Function to run SQL script in Snowflake
def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing in {schema} schema: {sql}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Function to archive old file content
def archive_old_file(file_name, old_content):
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.basename(file_name)
    archive_name = f"{base_name.split('.')[0]}_{timestamp}.sql"
    archive_file_path = os.path.join(ARCHIVE_DIR, archive_name)

    with open(archive_file_path, 'w') as f:
        f.write(old_content)

    print(f"🗄️ Archived old version of: {file_name} -> {archive_name}")

# Clean up archive files older than N days
def clean_old_archives():
    now = time.time()
    if not os.path.exists(ARCHIVE_DIR):
        return
    for file in os.listdir(ARCHIVE_DIR):
        file_path = os.path.join(ARCHIVE_DIR, file)
        if os.path.isfile(file_path):
            age = now - os.path.getmtime(file_path)
            if age > DAYS * 86400:
                os.remove(file_path)
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

# Deploy Tables to RPT schema
for file_name in sorted(os.listdir(TABLES_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(TABLES_FOLDER, file_name)
        with open(full_path, 'r') as f:
            current_content = f.read()
        current_hash = hashlib.sha256(current_content.encode()).hexdigest()

        previous_entry = file_hashes.get(file_name)
        previous_hash = previous_entry['hash'] if previous_entry else None

        if previous_hash == current_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
        else:
            if previous_entry:
                archive_old_file(full_path, previous_entry['content'])

            print(f"🚀 Running {file_name} in RPT schema")
            run_sql_script(cursor, full_path, 'RPT')
            file_hashes[file_name] = {
                "hash": current_hash,
                "content": current_content
            }
            print(f"✅ Done {file_name}")

# Deploy Stored Procedures to XFRM schema
for file_name in sorted(os.listdir(SP_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(SP_FOLDER, file_name)
        with open(full_path, 'r') as f:
            current_content = f.read()
        current_hash = hashlib.sha256(current_content.encode()).hexdigest()

        previous_entry = file_hashes.get(file_name)
        previous_hash = previous_entry['hash'] if previous_entry else None

        if previous_hash == current_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
        else:
            if previous_entry:
                archive_old_file(full_path, previous_entry['content'])

            print(f"🚀 Running {file_name} in XFRM schema")
            run_sql_script(cursor, full_path, 'XFRM')
            file_hashes[file_name] = {
                "hash": current_hash,
                "content": current_content
            }
            print(f"✅ Done {file_name}")

# Save updated hash + content data
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(file_hashes, f, indent=2)

# Close connection
cursor.close()
conn.close()
print("🎉 Deployment complete.")

# Clean up old archives
clean_old_archives()
