import os, hashlib, json, shutil, time
from datetime import datetime
import snowflake.connector

TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
HASH_TRACKER_FILE = '.deployed_data.json'
ARCHIVE_DIR = "./archive"
DAYS = 7

# Load previous deployed file data
if os.path.exists(HASH_TRACKER_FILE):
    with open(HASH_TRACKER_FILE, 'r') as f:
        deployed_data = json.load(f)
else:
    deployed_data = {}

def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing in {schema} schema:\n{sql}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

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

conn = snowflake.connector.connect(
    user=os.environ['DEV_SNOWFLAKE_USER'],
    password=os.environ['DEV_SNOWFLAKE_PASSWORD'],
    account=os.environ['DEV_SNOWFLAKE_ACCOUNT'],
    role=os.environ['DEV_SNOWFLAKE_ROLE'],
    warehouse=os.environ['DEV_SNOWFLAKE_WAREHOUSE'],
    database=os.environ['DEV_SNOWFLAKE_DATABASE'],
    schema='PUBLIC'
)

cursor = conn.cursor()

# Deploy Tables (RPT)
for file in sorted(os.listdir(TABLES_FOLDER)):
    if file.endswith('.sql'):
        path = os.path.join(TABLES_FOLDER, file)
        with open(path, 'r') as f: content = f.read()
        hash_val = hashlib.sha256(content.encode()).hexdigest()
        prev = deployed_data.get(file)
        if prev and prev['hash'] == hash_val:
            print(f"⏩ Skipping {file} (unchanged)")
        else:
            if prev: archive_old_version(path, prev['content'])
            run_sql_script(cursor, path, 'RPT')
            deployed_data[file] = {'hash': hash_val, 'content': content}
            print(f"✅ Done {file}")

# Deploy Stored Procedures (XFRM)
for file in sorted(os.listdir(SP_FOLDER)):
    if file.endswith('.sql'):
        path = os.path.join(SP_FOLDER, file)
        with open(path, 'r') as f: content = f.read()
        hash_val = hashlib.sha256(content.encode()).hexdigest()
        prev = deployed_data.get(file)
        if prev and prev['hash'] == hash_val:
            print(f"⏩ Skipping {file} (unchanged)")
        else:
            if prev: archive_old_version(path, prev['content'])
            run_sql_script(cursor, path, 'XFRM')
            deployed_data[file] = {'hash': hash_val, 'content': content}
            print(f"✅ Done {file}")

with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(deployed_data, f, indent=2)

cursor.close()
conn.close()
clean_old_archives()
print("🎉 Deployment complete.")
