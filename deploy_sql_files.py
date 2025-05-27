import os
import hashlib
import json
import time
from datetime import datetime
import snowflake.connector
from cryptography.hazmat.primitives import serialization

DAYS = 7
HASH_TRACKER_FILE = '.deployed_data.json'

# Load config JSON from env var
config_path = os.environ.get('CONFIG_FILE')
if not config_path or not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

with open(config_path, 'r') as f:
    config = json.load(f)

project_name = config.get("project_name")
snowflake_conf = config.get("snowflake")
folders_conf = config.get("folders")

if not project_name or not snowflake_conf or not folders_conf:
    raise ValueError("Missing required keys in config JSON")

# Load deployed data hash file
if os.path.exists(HASH_TRACKER_FILE):
    with open(HASH_TRACKER_FILE, 'r') as f:
        deployed_data = json.load(f)
else:
    deployed_data = {}

def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        file_data = f.read()
        return hashlib.sha256(file_data).hexdigest()

def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing in schema [{schema}]: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Clean up old archives function removed as per request

# Load private key from environment secret (write key file before running this script)
key_path = None
if 'key_path' in config:
    key_path = config['key_path']
else:
    raise ValueError("key_path not specified in config")

if not os.path.exists(key_path):
    raise FileNotFoundError(f"Private key file not found at {key_path}")

with open(key_path, "rb") as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )
private_key_bytes = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Connect to Snowflake using private key auth
conn = snowflake.connector.connect(
    user=snowflake_conf['user'],
    account=snowflake_conf['account'].split('.')[0],  # strip domain if present
    private_key=private_key_bytes,
    role=snowflake_conf['role'],
    warehouse=snowflake_conf['warehouse'],
    database=snowflake_conf['database'],
    schema='PUBLIC'  # initial schema, will switch in code
)

cursor = conn.cursor()

# Loop through each folder type in config (Tables, StoredProcs, Tasks)
for folder_type, folder_info in folders_conf.items():
    folder_path = folder_info.get("path")
    default_schema = folder_info.get("default_schema")

    if not folder_path or not default_schema:
        print(f"Skipping {folder_type} because path/schema not defined")
        continue

    if not os.path.exists(folder_path):
        print(f"Folder path {folder_path} does not exist, skipping {folder_type}")
        continue

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith('.sql'):
            continue

        full_path = os.path.join(folder_path, file_name)

        with open(full_path, 'r') as f:
            current_content = f.read()

        current_hash = hashlib.sha256(current_content.encode()).hexdigest()
        previous_data = deployed_data.get(file_name)

        if previous_data and previous_data['hash'] == current_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
            continue

        print(f"🚀 Running {file_name} in schema {default_schema}")
        try:
            run_sql_script(cursor, full_path, default_schema)
        except Exception as e:
            print(f"❌ Error executing {file_name}: {e}")
            cursor.close()
            conn.close()
            exit(1)

        # Save new hash + content
        deployed_data[file_name] = {
            'hash': current_hash,
            'content': current_content
        }
        print(f"✅ Done {file_name}")

# Save updated deployment data
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(deployed_data, f, indent=2)

# Close connection
cursor.close()
conn.close()
print("🎉 Deployment complete.")
