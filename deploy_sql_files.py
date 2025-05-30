import os
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Deploy project scripts to Snowflake")
    parser.add_argument('--project', required=True, help='Project name to deploy')
    args = parser.parse_args()

    project = args.project
    print(f"Starting deployment for project: {project}")

    # Print environment info (except private key)
    print(f"SNOWSQL_ACCOUNT: {os.getenv('SNOWSQL_ACCOUNT')}")
    print(f"SNOWSQL_USER: {os.getenv('SNOWSQL_USER')}")
    print(f"SNOWSQL_ROLE: {os.getenv('SNOWSQL_ROLE')}")
    print(f"SNOWSQL_WAREHOUSE: {os.getenv('SNOWSQL_WAREHOUSE')}")

    # Private key is sensitive - do NOT print

    try:
        # Here place your actual Snowflake deployment logic
        # Example: connect to Snowflake using snowflake-connector-python
        import snowflake.connector

        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWSQL_USER'),
            account=os.getenv('SNOWSQL_ACCOUNT'),
            private_key=os.getenv('SNOWSQL_PRIVATE_KEY'),
            role=os.getenv('SNOWSQL_ROLE'),
            warehouse=os.getenv('SNOWSQL_WAREHOUSE'),
            # Add more params as needed
        )
        cs = ctx.cursor()
        try:
            # Example query to test connection
            cs.execute("SELECT current_version()")
            one_row = cs.fetchone()
            print(f"Snowflake connection success, version: {one_row[0]}")

            # TODO: Add your deployment SQL execution logic here

        finally:
            cs.close()
            ctx.close()

        print("Deployment completed successfully.")

    except Exception as e:
        print(f"Error during deployment: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
