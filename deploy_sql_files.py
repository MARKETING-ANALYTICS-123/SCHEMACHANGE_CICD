import os
import sys
import json
import argparse
import snowflake.connector
from cryptography.hazmat.primitives import serialization

parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True)
parser.add_argument('--files', required=True)
parser.add_argument('--config_file', required=True)
parser.add_argument('--key_path', required=True)
parser.add_argument('--env', required=True, choices=['dev', 'prod'])
parser.add_argument('--replacements', required=False, help="Space-separated replacements, e.g. '_DEV=_PRD DEV_DB=PRD_DB'")
args = parser.parse_args()

if not os.path.isfile(args.config_file):
    print(f"❌ Config file not found at {args.config_file}")
    sys.exit(1)

with open(args.config_file, 'r') as f:
    config = json.load(f)

# Pick the environment config (dev/prod)
env_conf = config.get("environments", {}).get(args.env)
if env_conf is None:
    print(f"❌ Environment '{args.env}' not found in config")
    sys.exit(1)

snowflake_conf = env_conf.get("snowflake")
schemas_conf = env_conf.get("schemas")

if not os.path.isfile(args.key_path):
    print(f"❌ Private key file not found at {args.key_path}")
    sys.exit(1)

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

conn = snowflake.connector.connect(
    user=snowflake_conf['user'],
    account=snowflake_conf['account'].split('.')[0],
    private_key=private_key_bytes,
    role=snowflake_conf['role'],
    warehouse=snowflake_conf['warehouse'],
    database=snowflake_conf['database'],
    schema='XFRM'
)
cursor = conn.cursor()

replacements = {}
if args.replacements:
    for pair in args.replacements.split():
        if '=' in pair:
            k, v = pair.split('=', 1)
            replacements[k] = v

def run_sql_script(cursor, script_path, schema, replacements):
    with open(script_path, 'r') as f:
        sql = f.read()
    for old, new in replacements.items():
        sql = sql.replace(old, new)
    print(f"Executing {script_path} in schema [{schema}] with replacements: {replacements}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

changed_files = args.files.split()

deployed_any = False

for file_path in changed_files:
    parts = file_path.split('/')
    if len(parts) < 4:
        print(f"⚠️ Skipping invalid path: {file_path}")
        continue

    folder_type = parts[2]

    folder_info = schemas_conf.get(folder_type)
    if not folder_info:
        print(f"⚠️ No config for folder '{folder_type}', skipping {file_path}")
        continue

    schema = folder_info.get('schema', 'XFRM')

    if not os.path.exists(file_path):
        print(f"⚠️ File {file_path} does not exist locally, skipping.")
        continue

    try:
        run_sql_script(cursor, file_path, schema, replacements)
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
