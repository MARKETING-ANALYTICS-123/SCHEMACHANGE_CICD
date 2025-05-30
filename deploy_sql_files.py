import argparse
import os
import snowflake.connector

def deploy_sql_file(cursor, filepath):
    print(f"Deploying {filepath} ...")
    with open(filepath, 'r') as file:
        sql = file.read()
    try:
        cursor.execute(sql)
        print(f"Successfully deployed {filepath}")
    except Exception as e:
        print(f"Error deploying {filepath}: {e}")
        raise

def deploy_project(project_name, conn_params):
    base_path = f"dbscripts2/{project_name}"
    print(f"Starting deployment for project: {project_name}")
    
    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=conn_params['user'],
        account=conn_params['account'],
        private_key=conn_params['private_key'],
        role=conn_params['role'],
        warehouse=conn_params['warehouse'],
        database=conn_params.get('database'),
        schema=conn_params.get('schema'),
    )
    cs = ctx.cursor()
    
    try:
        for root, dirs, files in os.walk(base_path):
            for file in files:
                if file.endswith('.sql'):
                    full_path = os.path.join(root, file)
                    deploy_sql_file(cs, full_path)
    finally:
        cs.close()
        ctx.close()
    print(f"Deployment completed for project: {project_name}")

def main():
    parser = argparse.ArgumentParser(description="Deploy Snowflake SQL scripts for a project")
    parser.add_argument("--project", required=True, help="Project name to deploy")
    args = parser.parse_args()

    # Read connection params from env variables set in the GitHub Action
    conn_params = {
        'user': os.getenv('SNOWSQL_USER'),
        'account': os.getenv('SNOWSQL_ACCOUNT'),
        'private_key': os.getenv('SNOWSQL_PRIVATE_KEY'),
        'role': os.getenv('SNOWSQL_ROLE'),
        'warehouse': os.getenv('SNOWSQL_WAREHOUSE'),
        # Optional if you want to specify default DB/schema
        'database': os.getenv('SNOWSQL_DATABASE'),
        'schema': os.getenv('SNOWSQL_SCHEMA'),
    }

    # Validate connection params
    missing_params = [k for k,v in conn_params.items() if v is None and k in ['user', 'account', 'private_key', 'role', 'warehouse']]
    if missing_params:
        print(f"Missing Snowflake connection parameters: {missing_params}")
        exit(1)

    deploy_project(args.project, conn_params)

if __name__ == "__main__":
    main()
