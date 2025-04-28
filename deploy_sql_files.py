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
def load_file_hashes():
    if os.path.exists(HASH_TRACKER_FILE):
        with open(HASH_TRACKER_FILE, 'r') as f:
            return json.load(f)
    else:
        return {}

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
    cursor.execute(f"USE SCHEMA {schema};")  # Switch to the specific schema
    cursor.execute(sql)

# Snowflake connection setup
def setup_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema='PUBLIC'  # Default schema; it will switch as needed
    )

# Function to process files in a given folder (Tables or StoredProcs)
def process_files_in_folder(folder, schema, cursor, file_hashes):
    for file_name in sorted(os.listdir(folder)):
        if file_name.endswith('.sql'):
            full_path = os.path.join(folder, file_name)
            current_hash = get_file_hash(full_path)

            if file_name in file_hashes and file_hashes[file_name] == current_hash:
                print(f"⏩ Skipping {file_name} (unchanged)")
            else:
                print(f"🚀 Running {file_name} in {schema} schema")
                run_sql_script(cursor, full_path, schema)
                file_hashes[file_name] = current_hash
                print(f"✅ Done {file_name}")

# Function to archive old versions of files (excluding specific folders)
def archive_old_files():
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)

    for filename in os.listdir("."):
        if filename in ["archive", ".git", ".github"] or filename.startswith("."):
            continue

        if os.path.isfile(filename):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            archive_name = f"{filename}_{timestamp}"
            shutil.copy2(filename, os.path.join(ARCHIVE_DIR, archive_name))
            print(f"Archived old version of: {filename} -> {archive_name}")

# Function to clean up archived files older than 7 days
def clean_old_archives():
    now = time.time()

    for file in os.listdir(ARCHIVE_DIR):
        file_path = os.path.join(ARCHIVE_DIR, file)
        if os.path.isfile(file_path):
            age = now - os.path.getmtime(file_path)
            if age > DAYS * 86400:
                os.remove(file_path)
                print(f"Deleted: {file}")

# Main function
def main():
    # Load previous hash history
    file_hashes = load_file_hashes()

    # Setup Snowflake connection
    conn = setup_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Process files in Tables folder for RPT schema
        process_files_in_folder(TABLES_FOLDER, 'RPT', cursor, file_hashes)

        # Process files in StoredProcs folder for XFRM schema
        process_files_in_folder(SP_FOLDER, 'XFRM', cursor, file_hashes)

        # Save updated hash record
        with open(HASH_TRACKER_FILE, 'w') as f:
            json.dump(file_hashes, f, indent=2)

        print("🎉 Deployment complete.")

    finally:
        # Close Snowflake connection
        cursor.close()
        conn.close()

        # Archive old versions of files
        archive_old_files()

        # Clean up archived files older than 7 days
        clean_old_archives()

if __name__ == "__main__":
    main()
