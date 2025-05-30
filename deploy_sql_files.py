import os
import argparse
import subprocess
import json
import glob
import sys
from utils.snowflake import SnowflakeConnector

def load_config(project):
    with open(f"dbscripts2/{project}/config.json") as f:
        return json.load(f)

def find_changed_sql_files(project):
    result = subprocess.run(
        ['git', 'diff', '--name-only', 'HEAD^', 'HEAD'],
        stdout=subprocess.PIPE,
        check=True
    )
    changed_files = result.stdout.decode().splitlines()
    return [
        f for f in changed_files
        if f.startswith(f"dbscripts2/{project}/") and f.endswith(".sql")
    ]

def suspend_tasks(conn, task_names):
    for task in task_names:
        print(f"Suspending task: {task}")
        conn.execute(f"ALTER TASK IF EXISTS {task} SUSPEND")

def resume_tasks(conn, task_names):
    for task in task_names:
        print(f"Resuming task: {task}")
        conn.execute(f"ALTER TASK IF EXISTS {task} RESUME")

def deploy_sql_files(conn, sql_files):
    for file_path in sorted(sql_files):
        print(f"Deploying: {file_path}")
        with open(file_path) as f:
            sql = f.read()
            conn.execute(sql)

def collect_task_names(sql_files):
    task_names = []
    for path in sql_files:
        with open(path) as f:
            lines = f.readlines()
            for line in lines:
                if "create or replace task" in line.lower():
                    parts = line.strip().split()
                    if len(parts) >= 5:
                        task_names.append(parts[4])
    return task_names

def main(project):
    config = load_config(project)
    conn = SnowflakeConnector(
        account=os.environ["SNOWSQL_ACCOUNT"],
        user=os.environ["SNOWSQL_USER"],
        role=os.environ["SNOWSQL_ROLE"],
        warehouse=os.environ["SNOWSQL_WAREHOUSE"],
        private_key=os.environ["SNOWSQL_PRIVATE_KEY"]
    )

    changed_files = find_changed_sql_files(project)
    if not changed_files:
        print("No SQL files changed.")
        return

    task_files = [f for f in changed_files if "tasks" in f.lower()]
    task_names = collect_task_names(task_files)

    if task_names:
        suspend_tasks(conn, task_names)

    deploy_sql_files(conn, changed_files)

    if task_names:
        resume_tasks(conn, task_names)

    print("Deployment complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    args = parser.parse_args()
    main(args.project)
