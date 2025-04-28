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

# Load previous hash history
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
    # Read the SQL script
    with open(script_path, 'r') as f:
        sql = f.read()

    # Print which schema and SQL is being executed
    print(f"Executing in {schema} schema: {sql}")
        
    # Use the correct schema (either XFRM or RPT)
    cursor.execute(f"USE SCHEMA {schema};")  # Switch to the specific schema
        
    # Execute the SQL script
    cursor.execute(sql)

# Function to archive only changed files
def archive_changed_files():
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)

    for filename in os.listdir("."):
        # Skip directories and files we don't want to back up
        if filename in ["archive", ".git", ".github"] or filename.startswith("."):
            continue

        if os.path.isfile(filename):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            current_hash = get_file_hash(filename)

            # Only archive files that have changed (i.e., hash is different)
            if filename in file_hashes and file_hashes[filename] == current_hash:
                print(f"⏩ Skipping unchanged file: {filename}")
                continue  # Skip unchanged files

            # Archive changed files
            archive_name = f"{filename}_{timestamp}"
            shutil.copy2(filename, os.path.join(ARCHIVE_DIR, archive_name))
            print(f"Archived changed version of: {filename} -> {archive_name}")

            # Update the file hash tracker
            file_hashes[filename] = current_hash

# Function to clean up archived files older than a certain number of days
def clean_old_archives():
    now = time.time()

    for file in os.listdir(ARCHIVE_DIR):
        file_path = os.path.join(ARCHIVE_DIR, file)
        if os.path.isfile(file_path):
            age = now - os.path.getmtime(file_path)
            if age > DAYS * 86400:  # Days * seconds in a day (86400)
                os.remove(file_path)
                print(f"Deleted: {file}")

# Snowflake connection setup
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema='PUBLIC'  # Default schema; it will switch as needed
)

cursor = conn.cursor()

# Process SQL files in the Tables folder (to be deployed in RPT schema)
for file_name in sorted(os.listdir(TABLES_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(TABLES_FOLDER, file_name)
        current_hash = get_file_hash(full_path)
        
        if file_name in file_hashes and file_hashes[file_name] == current_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
        else:
            print(f"🚀 Running {file_name} in RPT schema")
            run_sql_script(cursor, full_path, 'RPT')  # Use RPT schema for tables
            file_hashes[file_name] = current_hash
            print(f"✅ Done {file_name}")

# Process SQL files in the StoredProcs folder (to be deployed in XFRM schema)
for file_name in sorted(os.listdir(SP_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(SP_FOLDER, file_name)
        current_hash = get_file_hash(full_path)
        
        if file_name in file_hashes and file_hashes[file_name] == current_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
        else:
            print(f"🚀 Running {file_name} in XFRM schema")
            run_sql_script(cursor, full_path, 'XFRM')  # Use XFRM schema for stored procedures
            file_hashes[file_name] = current_hash
            print(f"✅ Done {file_name}")

# Save updated hash record
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(file_hashes, f, indent=2)

# Close Snowflake connection
cursor.close()
conn.close()
print("🎉 Deployment complete.")

# Archive only changed files
archive_changed_files()

# Clean up archived files older than 7 days
clean_old_archives()
