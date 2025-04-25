import os
import hashlib
import json
import snowflake.connector

# Folder paths for tables and stored procedures
TABLES_FOLDER = 'dbscripts/Tables'
SP_FOLDER = 'dbscripts/StoredProcs'
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

# Snowflake connection
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

cursor.close()
conn.close()
print("🎉 Deployment complete.")
