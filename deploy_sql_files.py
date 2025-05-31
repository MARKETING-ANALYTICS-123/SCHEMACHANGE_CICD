import os
import json
import argparse
import base64
import snowflake.connector

def load_config(project_name):
    config_path = f"configs/{project_name}.json"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path) as f:
        return json.load(f)

def get_changed_files():
    changed = os.popen('git diff --name-only HEAD^ HEAD').read().splitlines()
    return [f for f in changed if f.endswith('.sql')]

def get_snowflake_connection(config):
    private_key_b64 = os.environ.get("PRIVATE_KEY")
    if not private_key_b64:
        raise Exception("PRIVATE_KEY environment variable not set")
    
    private_key = base64.b64decode(private_key_b64)
    pk_file = "/tmp/temp_key.p8"
    with open(pk_file, "wb") as f:
        f.write(private_key)

    return snowflake.connector.connect(
        user=config['snowflake']['user'],
        account=config['snowflake']['account'],
        role=config['snowflake']['role'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        private_key_file=pk_file
    )

def run_sql_file(cursor, file_path):
    with open(file_path, 'r') as f:
        sql = f.read()
    print(f"Executing {file_path}")
    cursor.execute(sql)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    args = parser.parse_args()
    project_name = args.project.upper()
    
    print(f"Starting deployment for project: {project_name}")
    config = load_config(project_name)
    changed_files = get_changed_files()
    print("Changed files:", changed_files)

    conn = get_snowflake_connection(config)
    cursor = conn.cursor()
    try:
        for section, details in config["folders"].items():
            folder_path = details["path"]
            schema = details.get("default_schema", "")
            for file_path in changed_files:
                if file_path.startswith(folder_path):
                    print(f"Deploying {file_path} to schema {schema}")
                    cursor.execute(f"USE SCHEMA {config['snowflake']['database']}.{schema}")
                    run_sql_file(cursor, file_path)
    finally:
        cursor.close()
        conn.close()
        print("Deployment complete.")

if __name__ == "__main__":
    main()
