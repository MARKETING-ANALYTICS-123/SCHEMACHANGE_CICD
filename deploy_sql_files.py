import os
import hashlib
import json
import re
import snowflake.connector
from cryptography.hazmat.primitives import serialization

HASH_TRACKER_FILE = '.deployed_data.json'

# Load config
config_path = os.environ.get('CONFIG_FILE')
if not config_path or not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

with open(config_path, 'r') as f:
    config = json.load(f)

project_name = config.get("project_name")
snowflake_conf = config.get("snowflake")
folders_conf = config.get("folders")

# Load private key
key_path = config.get("key_path")
if not os.path.exists(key_path):
    raise FileNotFoundError(f"Private key file not found at {key_path}")

with open(key_path, "rb") as key_file:
    p_key = serialization.load_pem_private_key(key_file.read(), password=None)

private_key_bytes = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_conf['user'],
    account=snowflake_conf['account'].split('.')[0],
    private_key=private_key_bytes,
    role=snowflake_conf['role'],
    warehouse=snowflake_conf['warehouse'],
    database=snowflake_conf['database'],
    schema='PUBLIC'  # We'll switch schema dynamically below
)

cursor = conn.cursor()

# Load deployment history
if os.path.exists(HASH_TRACKER_FILE):
    with open(HASH_TRACKER_FILE, 'r') as f:
        deployed_data = json.load(f)
else:
    deployed_data = {}

def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def run_sql_script(cursor, script_path, schema):
    with open(script_path, 'r') as f:
        sql = f.read()
    print(f"📄 Executing in schema [{schema}]: {script_path}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(sql)

def get_root_task(cursor, schema, child_task_name):
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute("SHOW TASKS")
    tasks = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    task_map = {task['name'].upper(): task for task in [dict(zip(columns, row)) for row in tasks]}

    current_name = child_task_name.split('.')[-1].upper()
    visited = set()
    prev = None
    while current_name in task_map:
        current_task = task_map[current_name]
        predecessor = current_task.get("predecessors")
        if not predecessor:
            return prev  # reached root, return previous task in chain
        prev = current_name
        current_name = re.sub(r'[^\w]', '', predecessor.split('.')[-1]).upper()
        if current_name in visited:
            raise Exception("Circular task dependency detected.")
        visited.add(current_name)
    return prev

def suspend_task(cursor, schema, task_name):
    print(f"🛑 Suspending task: {task_name}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(f'ALTER TASK "{task_name}" SUSPEND;')

def resume_task(cursor, schema, task_name):
    print(f"▶️ Resuming task: {task_name}")
    cursor.execute(f"USE SCHEMA {schema};")
    cursor.execute(f'ALTER TASK "{task_name}" RESUME;')

for folder_type, folder_info in folders_conf.items():
    folder_path = folder_info.get("path")
    schema = folder_info.get("default_schema")

    if not os.path.exists(folder_path):
        print(f"⚠️ Folder {folder_path} does not exist, skipping.")
        continue

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith('.sql'):
            continue

        full_path = os.path.join(folder_path, file_name)
        file_hash = get_file_hash(full_path)
        prev = deployed_data.get(full_path)

        if prev and prev['hash'] == file_hash:
            print(f"⏩ Skipping {file_name} (unchanged)")
            continue

        try:
            if folder_type == "tasks":
                task_name = file_name.replace('.sql', '').upper()
                root_task = get_root_task(cursor, schema, task_name)

                # Suspend root task if exists and different from current task
                if root_task and root_task != task_name:
                    suspend_task(cursor, schema, root_task)
                # If no root task (this task is root), suspend itself to update safely
                elif root_task is None:
                    suspend_task(cursor, schema, task_name)

            run_sql_script(cursor, full_path, schema)

            if folder_type == "tasks":
                # Resume root task first (if different)
                if root_task and root_task != task_name:
                    resume_task(cursor, schema, root_task)
                # Then resume current task
                resume_task(cursor, schema, task_name)

            deployed_data[full_path] = {'hash': file_hash}
            print(f"✅ Deployed {file_name}")

        except Exception as e:
            print(f"❌ Error deploying {file_name}: {e}")
            cursor.close()
            conn.close()
            exit(1)

# Save updated hash tracker
with open(HASH_TRACKER_FILE, 'w') as f:
    json.dump(deployed_data, f, indent=2)

cursor.close()
conn.close()
print("🎉 Deployment complete.")
