import os
import sys
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import json

# Config & env
config_path = os.environ.get('CONFIG_FILE')
if not config_path or not os.path.exists(config_path):
    print(f"❌ Config file not found at {config_path}")
    sys.exit(1)

with open(config_path, 'r') as f:
    config = json.load(f)

snowflake_conf = config.get("snowflake")
folders_conf = config.get("folders")

# Load private key
key_path = 'keys/temp_key.p8'
if not os.path.exists(key_path):
    print(f"❌ Private key file not found at {key_path}")
    sys.exit(1)

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

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_conf['user'],
    account=snowflake_conf['account'].split('.')[0],
    private_key=private_key_bytes,
    role=snowflake_conf['role'],
    warehouse=snowflake_conf['warehouse'],
    database=snowflake_conf['database'],
    schema='PUBLIC'
)
cursor = conn.cursor()

def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing {script_path} in schema [{schema}]")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Get changed files from env
changed_files_env = os.environ.get('CHANGED_FILES')
if not changed_files_env:
    print("❌ No changed files provided.")
    sys.exit(1)

changed_files = changed_files_env.split()

deployed_any = False

for file_path in changed_files:
    # Determine project folder from path: dbscripts2/PROJECT/FOLDER_TYPE/file.sql
    parts = file_path.split('/')
    if len(parts) < 4:
        print(f"⚠️ Skipping invalid path: {file_path}")
        continue

    project = parts[1]
    folder_type = parts[2]

    folder_info = folders_conf.get(folder_type.lower())
    if not folder_info:
        print(f"⚠️ No config found for folder type '{folder_type}', skipping {file_path}")
        continue

    schema = folder_info.get('default_schema')
    if not schema:
        print(f"⚠️ No default_schema for folder type '{folder_type}', skipping {file_path}")
        continue

    full_path = file_path
    if not os.path.exists(full_path):
        print(f"⚠️ File {full_path} does not exist locally, skipping.")
        continue

    try:
        run_sql_script(cursor, full_path, schema)
        print(f"✅ Deployed {file_path}")
        deployed_any = True
    except Exception as e:
        print(f"❌ Failed deploying {file_path}: {e}")
        cursor.close()
        conn.close()
        sys.exit(1)

cursor.close()
conn.close()

if deployed_any:
    print("🎉 Deployment complete.")
else:
    print("ℹ️ No files deployed.")
