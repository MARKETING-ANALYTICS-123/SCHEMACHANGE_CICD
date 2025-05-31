import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# Load config
config_path = os.environ.get('CONFIG_FILE')
if not config_path or not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

import json

with open(config_path, 'r') as f:
    config = json.load(f)

project_name = config.get("project_name")
snowflake_conf = config.get("snowflake")
schemas_conf = config.get("schemas")  # Adjusted to 'schemas' as per latest JSON

key_path = config.get("key_path")
if not os.path.exists(key_path):
    raise FileNotFoundError(f"Private key file not found at {key_path}")

# Load private key
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
    print(f"Executing in schema [{schema}]: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Get changed files from env var (space-separated list)
changed_files_str = os.environ.get('CHANGED_FILES', '')
changed_files = set(changed_files_str.split())

print("Changed files detected by Git:", changed_files)

# Deploy only changed files
for schema_name, schema_info in schemas_conf.items():
    schema_path = schema_info.get("path")
    objects = schema_info.get("objects", [])

    if not os.path.exists(schema_path):
        print(f"⚠️ Schema path {schema_path} does not exist, skipping.")
        continue

    for obj_type in objects:
        obj_path = os.path.join(schema_path, obj_type)
        if not os.path.exists(obj_path):
            print(f"⚠️ Object path {obj_path} does not exist, skipping.")
            continue

        for file_name in sorted(os.listdir(obj_path)):
            if not file_name.endswith('.sql'):
                continue

            full_path = os.path.join(obj_path, file_name)

            # Construct repo relative path for comparison
            # Assuming script runs from repo root
            repo_relative_path = os.path.relpath(full_path, os.getcwd())

            if repo_relative_path not in changed_files:
                print(f"⏩ Skipping {file_name} (not changed)")
                continue

            try:
                run_sql_script(cursor, full_path, schema_name)
                print(f"✅ Deployed {file_name}")
            except Exception as e:
                print(f"❌ Error deploying {file_name}: {e}")
                cursor.close()
                conn.close()
                exit(1)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
