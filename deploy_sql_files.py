import os
import argparse
import json
import sys
import base64

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

import snowflake.connector

def load_private_key_from_str(key_str):
    """
    Load private key object from PEM string.
    If key is base64 encoded in secret, decode it first.
    """
    # If key is base64 encoded, uncomment below line and comment next:
    # key_bytes = base64.b64decode(key_str)
    
    key_bytes = key_str.encode('utf-8')  # if PEM string directly
    
    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=None,
        backend=default_backend()
    )
    return private_key

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    project = args.project.upper()

    # Read config JSON for project
    config_path = f"configs/{project}.json"
    if not os.path.isfile(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    sf_conf = config.get("snowflake", {})

    # Read private key string from env variable (GitHub Secret)
    private_key_str = os.getenv("SNOWSQL_PRIVATE_KEY")
    if not private_key_str:
        print("Error: SNOWSQL_PRIVATE_KEY env var not set")
        sys.exit(1)

    # Load private key object from string
    try:
        private_key_obj = load_private_key_from_str(private_key_str)
    except Exception as e:
        print(f"Failed to load private key from string: {e}")
        sys.exit(1)

    # Connect to Snowflake using key pair auth
    try:
        ctx = snowflake.connector.connect(
            user=sf_conf.get("user"),
            account=sf_conf.get("account"),
            role=sf_conf.get("role"),
            warehouse=sf_conf.get("warehouse"),
            database=sf_conf.get("database"),
            private_key=private_key_obj
        )
        cs = ctx.cursor()
        print(f"Connected to Snowflake account {sf_conf.get('account')} as user {sf_conf.get('user')}")
    except Exception as e:
        print(f"Snowflake connection error: {e}")
        sys.exit(1)

    # Get changed files from env var set in GitHub Actions
    changed_files_str = os.getenv("CHANGED_FILES", "")
    if not changed_files_str:
        print("No CHANGED_FILES env var set. Exiting.")
        sys.exit(0)

    changed_files = changed_files_str.split()

    # Filter changed SQL files for this project under dbscripts2/project/...
    relevant_files = [
        f for f in changed_files
        if f.startswith(f"dbscripts2/{project}/") and f.endswith(".sql")
    ]

    if not relevant_files:
        print("No changed SQL files detected for project.")
        sys.exit(0)

    print(f"Deploying {len(relevant_files)} changed SQL files for project {project}:")

    for file_path in relevant_files:
        print(f"Deploying {file_path} ...")
        try:
            with open(file_path) as f:
                sql = f.read()
            cs.execute(sql)
            print(f"Successfully executed {file_path}")
        except Exception as e:
            print(f"Failed to execute {file_path}: {e}")
            cs.close()
            ctx.close()
            sys.exit(1)

    cs.close()
    ctx.close()
    print("Deployment completed successfully.")

if __name__ == "__main__":
    main()
