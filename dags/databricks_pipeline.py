from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.dates import days_ago
import base64
import requests
import json

# --- CONFIGURATION (UPDATE THESE) ---
# 1. Your Cluster ID (From Azure Databricks -> Compute -> JSON)
CLUSTER_ID = "1208-092236-airf5tpn" 

# 2. Your Notebook Path (From Azure Databricks Workspace)
# Example: /Users/yourname@email.com/process_orders_delta
NOTEBOOK_PATH = "/Users/mond.phakapol@gmail.com/process_orders_delta"
# ------------------------------------

LOCAL_FILE_PATH = "/opt/airflow/dags/latest_orders.csv"
DBFS_TARGET_PATH = "/FileStore/tables/latest_orders.csv"

def upload_file_to_dbfs(**kwargs):
    """
    Uploads a file to Azure Databricks DBFS using the REST API.
    We use DatabricksHook to securely get the Host and Token.
    """
    hook = DatabricksHook(databricks_conn_id='databricks_default')
    conn = hook.get_connection('databricks_default')
    
    # API Endpoint for uploading files
    url = f"https://{conn.host}/api/2.0/dbfs/put"
    
    # Read the file and encode it to base64 (Required by API)
    with open(LOCAL_FILE_PATH, "rb") as file:
        encoded_content = base64.b64encode(file.read()).decode('utf-8')
    
    # Prepare the payload
    payload = {
        "path": DBFS_TARGET_PATH,
        "contents": encoded_content,
        "overwrite": True
    }
    
    # Send the Request
    print(f"🚀 Uploading {LOCAL_FILE_PATH} to {DBFS_TARGET_PATH}...")
    response = requests.post(
        url, 
        headers={"Authorization": f"Bearer {conn.password}"}, 
        json=payload
    )
    
    if response.status_code == 200:
        print("✅ Upload Success!")
    else:
        raise Exception(f"❌ Upload Failed: {response.text}")

with DAG(
    dag_id="03_deploy_to_azure_databricks",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lmwn_project", "azure", "spark"],
) as dag:

    # Task 1: Generate Data (Reuse your script)
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/data_generator.py"
    )

    # Task 2: Upload to DBFS (Python Function)
    upload_to_dbfs = PythonOperator(
        task_id="upload_to_dbfs",
        python_callable=upload_file_to_dbfs
    )

    # Task 3: Trigger Spark Notebook
    run_spark_job = DatabricksSubmitRunOperator(
        task_id='run_spark_job',
        databricks_conn_id='databricks_default',
        existing_cluster_id=CLUSTER_ID,
        notebook_task={
            'notebook_path': NOTEBOOK_PATH,
        }
    )

    generate_data >> upload_to_dbfs >> run_spark_job