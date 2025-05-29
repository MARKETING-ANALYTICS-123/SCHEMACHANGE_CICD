import os
import json
import subprocess
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

# Connect to Snowflake (schema omitted to allow per-script control)
conn = snowflake.connector.connect(
    user=snowflake_conf['user'],
    account=snowflake_conf['account'].split('.')[0],
    private_key=private_key_bytes,
    role=snowflake_conf['role'],
    warehouse=snowflake_conf['warehouse'],
    database=snowflake_conf['database']
)

cursor = conn.cursor()

# Get changed .sql files using git diff from base branch
def get_changed_sql_files(base_branch="origin/main"):
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_branch}...HEAD", "--", "*.sql"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError("Failed to get changed files from git")
    return {os.path.normpath(f.strip()) for f in result.stdout.splitlines() if f.strip()}

changed_files = get_changed_sql_files()

# Run SQL script in specified schema
def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing in schema [{schema}]: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)
    print(f"✅ Deployed {os.path.basename(script_path)}")

# Deploy scripts from folders defined in config
for folder_type, folder_info in folders_conf.items():
    folder_path = folder_info.get("path")
    schema = folder_info.get("default_schema")

    if not os.path.exists(folder_path):
        print(f"⚠️ Folder {folder_path} does not exist, skipping.")
        continue

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith('.sql'):
            continue

        full_path = os.path.join(folder_path, file_name)
        rel_path = os.path.normpath(full_path)

        if rel_path not in changed_files:
            continue  # Skip unchanged files

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
