import os
import subprocess
import snowflake.connector
 
# Folder paths
TABLES_FOLDER = 'dbscripts2/Tables'
SP_FOLDER = 'dbscripts2/StoredProcs'
 
# Function to get list of changed files from Git
def get_changed_files():
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        stdout=subprocess.PIPE,
        text=True
    )
    files = result.stdout.strip().split('\n')
    return [f for f in files if f.endswith('.sql')]
 
# Function to run SQL script in Snowflake
def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"Executing in {schema} schema: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)
 
# Snowflake connection
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    role=os.environ['SNOWFLAKE_ROLE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema='PUBLIC'
)
cursor = conn.cursor()
 
# Main deployment
changed_files = get_changed_files()
 
if not changed_files:
    print("🚫 No SQL file changes detected. Nothing to deploy.")
else:
    for file_path in changed_files:
        full_path = os.path.abspath(file_path)
        if TABLES_FOLDER in file_path:
            print(f"🚀 Deploying Table script: {file_path}")
            run_sql_script(cursor, full_path, 'RPT')
            print(f"✅ Done {file_path}")
        elif SP_FOLDER in file_path:
            print(f"🚀 Deploying Stored Procedure script: {file_path}")
            run_sql_script(cursor, full_path, 'XFRM')
            print(f"✅ Done {file_path}")
        else:
            print(f"⚠️ Skipping unrelated file: {file_path}")
 
# Close connection
cursor.close()
conn.close()
print("🎉 Deployment complete for changed files only!")
