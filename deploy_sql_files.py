import os
import hashlib
import json
import snowflake.connector

SQL_FOLDER = 'dbscripts2'
HASH_TRACKER_FILE = '.deployed_hashes.json'

# Load previous hash history
if os.path.exists(HASH_TRACKER_FILE):
    with open(HASH_TRACKER_FILE, 'r') as f:
        file_hashes = json.load(f)
else:
    file_hashes = {}

def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        file_data = f.read()
        return hashlib.sha256(file_data).hexdigest()

def run_sql_script(cursor, script_path):
    with open(script_path, 'r') as f:
        sql = f.read()
        cursor.execute(sql)

# Snowflake connection
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

# Go through each .sql file
for file_name in sorted(os.listdir(SQL_FOLDER)):
    if file_name.endswith('.sql'):
        full_path = os.path.join(SQL_FOLDER, file_name)
        current_hash = get_file_hash(full_path)
        
        if file_name in file_hashes and file_hashes[file_name] == current_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
        else:
            print(f"🚀 Running {file_name}")
            run_sql_script(cursor, full_path)
            file_hashes[file_name] = current_hash
            print(f"✅ Done {file_name}")

# Save updated hash record
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(file_hashes, f, indent=2)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
