import os
import json
import hashlib
import subprocess
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def get_changed_sql_files():
    result = subprocess.run(
        ['git', 'diff', '--name-only', 'origin/PROD...HEAD', '--', '*.sql'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git diff failed: {result.stderr}")
    files = result.stdout.strip().split('\n')
    return [f for f in files if f.endswith('.sql')]

def get_file_hash(path):
    with open(path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing: {script_path} in schema [{schema}]")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Read config
config_path = os.environ.get("CONFIG_FILE")
if not config_path or not os.path.exists(config_path):
    raise FileNotFoundError(f"Missing or invalid config: {config_path}")

with open(config_path, 'r') as f:
    config = json.load(f)

project_name = config['project_name']
folders_conf = config['folders']
sf_conf = config['snowflake']

# Load private key
key_path = config.get("key_path")
with open(key_path, "rb") as key_file:
    p_key = serialization.load_pem_private_key(key_file.read(), password=None)

private_key_bytes = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf_conf['user'],
    account=sf_conf['account'].split('.')[0],
    private_key=private_key_bytes,
    role=sf_conf['role'],
    warehouse=sf_conf['warehouse'],
    database=sf_conf['database'],
)
cursor = conn.cursor()

# Determine changed SQL files
changed_files = get_changed_sql_files()

# Deploy only changed files
for folder_type, folder_info in folders_conf.items():
    folder_path = folder_info["path"]
    schema = folder_info["default_schema"]

    for file in changed_files:
        if file.startswith(folder_path) and file.endswith(".sql"):
            try:
                run_sql_script(cursor, file, schema)
                print(f"✅ Deployed: {file}")
            except Exception as e:
                print(f"❌ Failed to deploy {file}: {e}")
                cursor.close()
                conn.close()
                exit(1)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
