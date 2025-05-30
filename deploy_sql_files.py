import os
import json
import argparse
import snowflake.connector

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='Project name')
    args = parser.parse_args()

    project = args.project

    # Load config JSON for the project
    config_path = f'configs/{project}.json'
    with open(config_path) as f:
        config = json.load(f)

    snowflake_conf = config['snowflake']
    key_path = config['key_path']

    # Read the private key file content (assuming it's in repo)
    with open(key_path, 'rb') as key_file:
        private_key = key_file.read()

    # Or if you want to use the private key from environment variable (base64 encoded for example)
    # private_key_env = os.getenv('SNOWSQL_PRIVATE_KEY')
    # You can decode it here if needed

    # Connect to Snowflake using key pair auth
    ctx = snowflake.connector.connect(
        user=snowflake_conf['user'],
        account=snowflake_conf['account'],
        role=snowflake_conf['role'],
        warehouse=snowflake_conf['warehouse'],
        database=snowflake_conf.get('database'),
        private_key=private_key,
        authenticator='snowflake'  # or any other as required
    )

    print(f"Connected to Snowflake account {snowflake_conf['account']} as user {snowflake_conf['user']}")

    # Your deployment logic here, example:
    # - Scan folders in config['folders']
    # - Read SQL files
    # - Execute them via ctx.cursor()

    # Close connection
    ctx.close()

if __name__ == "__main__":
    main()
