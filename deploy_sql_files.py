import os
import sys
import json
import argparse
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# Parse CLI arguments
parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True)
parser.add_argument('--files', required=True)
parser.add_argument('--config_file', required=True)
parser.add_argument('--key_path', required=True)
args = parser.parse_args()

# Validate config file
if not os.path.isfile(args.config_file):
    print(f"❌ Config file not found at {args.config_file}")
    sys.exit(1)

# Load config
with open(args.config_file, 'r') as f:
    config = json.load(f)

snowflake_conf = config.get("snowflake")

# Validate private key file
if not os.path.isfile(args.key_path):
    print(f"❌ Private key file not found at {args.key_path}")
    sys.exit(1)

# Load and deserialize private key
with open(args.key_path, "rb") as key_file:
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
    schema='PUBLIC'  # Initial schema, will be overridden per script
)
cursor = conn.cursor()

# Helper to run SQL from file
def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing {script_path} in schema [{schema}]")
    cursor.execute(f"USE DATABASE {snowflake_conf['database']};")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

# Process changed files
changed_files = args.files.split()
deployed_any = False

for file_path in changed_files:
    parts = file_path.split('/')
    if len(parts) < 4:
        print(f"⚠️ Skipping invalid path: {file_path}")
        continue

    schema = parts[2]  # Use folder name directly as schema name

    if not os.path.exists(file_path):
        print(f"⚠️ File {file_path} does not exist locally, skipping.")
        continue

    try:
        run_sql_script(cursor, file_path, schema)
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
