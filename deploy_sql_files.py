import os
import re
import yaml
import snowflake.connector
from collections import defaultdict

def parse_task_name(sql_text):
    """Extract task name from SQL (CREATE OR REPLACE TASK <name>)"""
    match = re.search(r"CREATE\s+OR\s+REPLACE\s+TASK\s+([^\s]+)", sql_text, re.IGNORECASE)
    return match.group(1).strip() if match else None

def get_task_dependencies(sql_text):
    """Extract tasks that the current task depends on (AFTER clause)"""
    return re.findall(r"AFTER\s+([a-zA-Z0-9_\.]+)", sql_text, re.IGNORECASE)

def load_project_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def read_sql_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()

def topological_sort(tasks):
    visited = {}
    order = []

    def visit(task):
        if task in visited:
            if visited[task] == 1:
                raise ValueError("Cycle detected in task dependencies")
            return
        visited[task] = 1
        for dep in tasks[task]['depends_on']:
            visit(dep)
        visited[task] = 2
        order.append(task)

    for task in tasks:
        visit(task)
    return order

def deploy_sql_scripts(project_config_path, changed_files):
    config = load_project_config(project_config_path)

    conn = snowflake.connector.connect(
        user=config["user"],
        account=config["account"],
        private_key=config["private_key"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        role=config["role"]
    )

    cursor = conn.cursor()

    try:
        # Parse task scripts to gather task graph
        task_graph = {}
        task_file_map = {}

        for file_path in changed_files:
            if '/tasks/' in file_path.lower() and file_path.endswith('.sql'):
                sql_text = read_sql_file(file_path)
                task_name = parse_task_name(sql_text)
                if task_name:
                    depends_on = get_task_dependencies(sql_text)
                    task_graph[task_name] = {"depends_on": depends_on}
                    task_file_map[task_name] = file_path

        sorted_tasks = topological_sort(task_graph)

        # Find root tasks to suspend (no parent tasks)
        root_tasks = [t for t, v in task_graph.items() if not v["depends_on"]]
        root_status = {}

        for task in root_tasks:
            cursor.execute(f"SHOW TASKS LIKE '{task}'")
            result = cursor.fetchone()
            if result and result[6] == "started":
                cursor.execute(f"ALTER TASK {task} SUSPEND")
                root_status[task] = "started"
            else:
                root_status[task] = "suspended"

        # Deploy tasks in topological order
        for task in sorted_tasks:
            file_path = task_file_map[task]
            sql = read_sql_file(file_path)
            print(f"Deploying task: {task}")
            cursor.execute(sql)

        # Resume only previously started root tasks
        for task in root_tasks:
            if root_status.get(task) == "started":
                cursor.execute(f"ALTER TASK {task} RESUME")

        # Deploy other SQLs (e.g., tables, views, procedures)
        for file_path in changed_files:
            if not '/tasks/' in file_path.lower() and file_path.endswith('.sql'):
                sql = read_sql_file(file_path)
                print(f"Deploying object from: {file_path}")
                cursor.execute(sql)

    finally:
        cursor.close()
        conn.close()
