import os
import json
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# Load config
config_path = os.environ.get('CONFIG_FILE')
if not config_path or not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

with open(config_path, 'r') as f:
    config = json.load(f)

project_name = config.get("project_name")
snowflake_conf = config.get("snowflake")
folders_conf = config.get("folders")

# Load private key
key_path = config.get("key_path")
if not os.path.exists(key_path):
    raise FileNotFoundError(f"Private key file not found at {key_path}")

with open(key_path, "rb") as key_file:
    p_key = serialization.load_pem_private_key(key_file.read(), password=None)

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
    database=snowflake_conf['database']
)

cursor = conn.cursor()

# Load changed files from GitHub Action step
changed_files_file = 'changed_files.txt'
if not os.path.exists(changed_files_file):
    print("⚠️ No changed_files.txt found.")
    exit(0)

with open(changed_files_file, 'r') as f:
    changed_files = {os.path.normpath(line.strip()) for line in f if line.strip().endswith('.sql')}

if not changed_files:
    print("ℹ️ No .sql files changed. Nothing to deploy.")
    exit(0)

# Run SQL script in specified schema
def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing in schema [{schema}]: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)
    print(f"✅ Deployed {os.path.basename(script_path)}")

# Deploy only changed files
for folder_type, folder_info in folders_conf.items():
    folder_path = folder_info.get("path")
    schema = folder_info.get("default_schema")

    if not os.path.exists(folder_path):
        print(f"⚠️ Folder {folder_path} does not exist, skipping.")
        continue

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith('.sql'):
            continue

        full_path = os.path.normpath(os.path.join(folder_path, file_name))
        if full_path not in changed_files:
            continue

        try:
            run_sql_script(cursor, full_path, schema)
        except Exception as e:
            print(f"❌ Error deploying {file_name}: {e}")
            cursor.close()
            conn.close()
            exit(1)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
