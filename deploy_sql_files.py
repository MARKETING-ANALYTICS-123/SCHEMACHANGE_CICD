import os
import json
import sys
import tempfile
from cryptography.hazmat.primitives import serialization
import snowflake.connector

def get_snowflake_connection(config, private_key_pem):
    # Write key to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as key_file:
        key_file.write(private_key_pem)
        key_file_path = key_file.name

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None,
    )

    return snowflake.connector.connect(
        account=config['snowflake']['account'],
        user=config['snowflake']['user'],
        role=config['snowflake']['role'],
        warehouse=config['snowflake']['warehouse'],
        private_key=private_key
    )

def main():
    project = os.environ.get("PROJECT")
    if not project:
        raise ValueError("PROJECT env var is not set")

    print(f"Starting deployment for project: {project}")

    config_path = f"configs/{project}.json"
    with open(config_path) as f:
        config = json.load(f)

    changed_files = os.environ.get("CHANGED_FILES", "").split(",")
    print(f"Changed files: {changed_files}")

    private_key_pem = os.environ.get("PRIVATE_KEY")
    if not private_key_pem:
        raise ValueError("Missing PRIVATE_KEY in environment")

    conn = get_snowflake_connection(config, private_key_pem)

    # Your logic to execute SQL files goes here
    # For now just simulate
    print("✅ Connected to Snowflake successfully.")

if __name__ == "__main__":
    main()
