import os
import hashlib
import json
import snowflake.connector
from cryptography.hazmat.primitives import serialization

HASH_TRACKER_FILE = '.deployed_data.json'

# Load config
config_path = os.environ.get('CONFIG_FILE')
if not config_path or not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

with open(config_path, 'r') as f:
    config = json.load(f)

project_name = config.get("project_name")
snowflake_conf = config.get("snowflake")
schemas_conf = config.get("schemas")

# Load private key
key_path = config.get("key_path")
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

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_conf['user'],
    account=snowflake_conf['account'].split('.')[0],
    private_key=private_key_bytes,
    role=snowflake_conf['role'],
    warehouse=snowflake_conf['warehouse'],
    database=snowflake_conf['database'],
    schema='PUBLIC'  # default schema, overridden in deploy step
)

cursor = conn.cursor()

# Load file change history
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
    print(f"Executing in schema [{schema}]: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Deploy scripts based on config structure
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
            file_hash = get_file_hash(full_path)
            prev = deployed_data.get(full_path)

            if prev and prev['hash'] == file_hash:
                print(f"⏩ Skipping {file_name} (unchanged)")
                continue

            try:
                run_sql_script(cursor, full_path, schema_name)
                deployed_data[full_path] = {'hash': file_hash}
                print(f"✅ Deployed {file_name}")
            except Exception as e:
                print(f"❌ Error deploying {file_name}: {e}")
                cursor.close()
                conn.close()
                exit(1)

# Save hash tracker
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(deployed_data, f, indent=2)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
