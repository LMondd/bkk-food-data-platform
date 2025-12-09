from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# --- CONFIGURATION (UPDATE THESE!) ---
# 1. Your actual bucket name from Step 1
BUCKET_NAME = "lmwn-lake-mond"  
# 2. Your project ID (Found in GCP dashboard, e.g., "lmwn-intern-123")
GCP_PROJECT_ID = "lmwn-project" 

DATASET_NAME = "lmwn_analytics"
TABLE_NAME = "orders"
# -------------------------------------

with DAG(
    dag_id="02_deploy_to_cloud",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lmwn_project", "gcp"],
) as dag:

    # Task 1: Generate Data (Updated to create 'latest_orders.csv')
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/data_generator.py"
    )

    # Task 2: Upload local CSV to Google Cloud Storage
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/opt/airflow/dags/latest_orders.csv",  # The file created by Task 1
        dst="raw/orders/latest_orders.csv",         # Where it goes in the cloud
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_default"
    )

    # Task 3: Load CSV from GCS to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=BUCKET_NAME,
        source_objects=["raw/orders/latest_orders.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE", # Overwrite table for testing
        source_format="CSV",
        skip_leading_rows=1, 
        autodetect=True,     # Let BigQuery guess schema
        gcp_conn_id="google_cloud_default"
    )

    generate_data >> upload_to_gcs >> load_to_bq