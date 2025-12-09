from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import glob
import os

# SETTINGS
DATA_PATH = "/opt/airflow/dags/"
DB_CONN = "postgres_default"

# 1. Define the function explicitly (Fixes the Lambda/Pickle error)
def create_table_callable():
    """Creates the table if it does not exist."""
    sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_name VARCHAR(100),
            restaurant_name VARCHAR(100),
            category VARCHAR(50),
            payment_method VARCHAR(50),
            total_amount FLOAT,
            delivery_fee FLOAT,
            order_timestamp TIMESTAMP,
            customer_lat FLOAT,
            customer_long FLOAT,
            restaurant_lat FLOAT,
            restaurant_long FLOAT,
            status VARCHAR(20)
        );
    """
    # We use the hook to run the SQL safely
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    hook.run(sql)
    print("Table 'orders' created successfully (or already existed).")


def load_data_to_postgres():
    """Finds the latest CSV and loads it to Postgres with Error Handling."""
    try:
        # Find all CSV files
        files = glob.glob(f"{DATA_PATH}lmwn_mock_orders_*.csv")
        
        if not files:
            print("❌ No files found! Run the generator first.")
            return
        
        # Get the newest file
        latest_file = max(files, key=os.path.getctime)
        print(f"📂 Processing file: {latest_file}")
        
        # DEBUG STEP 1: Check file size
        file_size = os.path.getsize(latest_file)
        print(f"📊 File Size: {file_size} bytes")

        if file_size == 0:
            raise ValueError("The CSV file is empty!")

        # DEBUG STEP 2: Read CSV
        df = pd.read_csv(latest_file, encoding='utf-8')
        print(f"✅ Read {len(df)} rows from CSV.")
        
        # DEBUG STEP 3: Connect to DB
        print("🔌 Connecting to Postgres...")
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # DEBUG STEP 4: Upload
        print("🚀 Uploading to table 'orders'...")
        df.to_sql('orders', engine, if_exists='append', index=False)
        print("✅ Data successfully loaded to Postgres!")
        
    except Exception as e:
        print(f"💥 ERROR OCCURRED: {e}")
        # We print the full error so it shows in the log
        import traceback
        traceback.print_exc()
        raise e

# 2. DEFINE DAG
with DAG(
    dag_id="01_ingest_mock_data",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lmwn_project"],
) as dag:

    # TASK 1: Create Table (Updated to use proper function)
    create_table_task = PythonOperator(
        task_id="create_postgres_table",
        python_callable=create_table_callable
    )

    # TASK 2: Generate Data
    generate_data_task = BashOperator(
        task_id="generate_mock_csv",
        bash_command="python /opt/airflow/scripts/data_generator.py"
    )

    # TASK 3: Load Data
    load_to_db_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_data_to_postgres
    )

    create_table_task >> generate_data_task >> load_to_db_task