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

# Get list of changed files from env
changed_files = os.environ.get("CHANGED_FILES", "").split()
if not changed_files:
    print("❌ No changed files provided.")
    exit(1)

# Map paths to schema based on folder config
for folder_key, folder_info in folders_conf.items():
    folder_path = folder_info.get("path")
    schema = folder_info.get("default_schema")

    if not os.path.exists(folder_path):
        print(f"⚠️ Folder {folder_path} does not exist, skipping.")
        continue

    for file_path in changed_files:
        if not file_path.startswith(folder_path + "/") or not file_path.endswith(".sql"):
            continue

        if not os.path.exists(file_path):
            print(f"⚠️ File {file_path} not found, skipping.")
            continue

        try:
            with open(file_path, "r") as f:
                sql = f.read()
            print(f"🚀 Deploying {file_path} to schema {schema}")
            cursor.execute(f"USE SCHEMA {schema};")
            cursor.execute(sql)
            print(f"✅ Deployed {file_path}")
        except Exception as e:
            print(f"❌ Error deploying {file_path}: {e}")
            cursor.close()
            conn.close()
            exit(1)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
