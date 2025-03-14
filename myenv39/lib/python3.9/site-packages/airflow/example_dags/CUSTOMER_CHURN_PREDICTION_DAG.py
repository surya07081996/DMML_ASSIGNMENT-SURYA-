from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Define the root directory where the scripts are located
ROOT_DIR = ""

# Function to execute index.py dynamically
def run_script(folder_name):
    script_path = os.path.join(ROOT_DIR, folder_name, "index.py")
    try:
        if os.path.exists(script_path):
            exec(open(script_path).read(), globals())
            print(f"✅ Successfully executed: {script_path}")
        else:
            raise FileNotFoundError(f"❌ Script not found: {script_path}")
    except Exception as e:
        print(f"❌ Error executing {script_path}: {e}")
        raise


# Define DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 14),
    "catchup": False,
}

dag = DAG(
    "CUSTOMER_CHURN_PREDICTION_DAG",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,  # Prevents running past dates
)

# Define task sequence
steps = [
    "1_data_ingestion",
    "2_raw_data_storage",
    "3_data_validation",
    "4_data_preparation",
    "5_data_transformation_and_storage",
    "6_feature_storage",
    "8_model_building",
]

tasks = []
for step in steps:
    task = PythonOperator(
        task_id=step,
        python_callable=run_script,
        op_args=[step],
        dag=dag,
    )
    tasks.append(task)

# Set task dependencies (run sequentially)
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
