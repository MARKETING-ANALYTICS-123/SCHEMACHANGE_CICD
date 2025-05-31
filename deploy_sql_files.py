import os
import json
import base64
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_snowflake_connection(config, private_key_pem):
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None,
        backend=default_backend()
    )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=config["snowflake"]["account"],
        user=config["snowflake"]["user"],
        private_key=private_key_bytes,
        role=config["snowflake"]["role"],
        warehouse=config["snowflake"]["warehouse"],
        database=config["snowflake"]["database"]
    )

def execute_sql_file(cursor, file_path):
    with open(file_path, 'r') as file:
        sql = file.read()
        for statement in sql.strip().split(';'):
            if statement.strip():
                cursor.execute(statement)

def main():
    project = os.environ.get("PROJECT")
    if not project:
        raise Exception("PROJECT environment variable not set.")

    print(f"Starting deployment for project: {project}")

    config_path = f"configs/{project}.json"
    with open(config_path) as f:
        config = json.load(f)

    private_key_env = os.environ.get("PRIVATE_KEY")
    if not private_key_env:
        raise Exception("PRIVATE_KEY secret not found in environment.")
    private_key_pem = private_key_env.replace('\\n', '\n')

    changed_files = os.environ.get("CHANGED_FILES", "").split(',')
    changed_files = [f for f in changed_files if f.endswith(".sql")]

    print("Changed files:", changed_files)

    conn = get_snowflake_connection(config, private_key_pem)
    cursor = conn.cursor()

    try:
        for folder_key, folder_conf in config["folders"].items():
            folder_path = folder_conf["path"]
            default_schema = folder_conf.get("default_schema", "")
            for file in changed_files:
                if file.startswith(folder_path):
                    print(f"Deploying {file} to schema {default_schema}")
                    cursor.execute(f"USE SCHEMA {config['snowflake']['database']}.{default_schema}")
                    execute_sql_file(cursor, file)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
