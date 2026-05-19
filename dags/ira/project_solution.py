from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import sys

# Final Project Part 2 — Airflow DAG
# Runs landing→bronze→silver→gold pipeline

default_args = {
    "owner": "ira",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

def run_script(script_name):
    """Run a Python script as a subprocess."""
    result = subprocess.run(
        [sys.executable, f"/opt/airflow/dags/fp/{script_name}"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"{script_name} failed with return code {result.returncode}")

with DAG(
    dag_id="fp_datalake_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["final_project", "part2"],
) as dag:

    landing_to_bronze = PythonOperator(
        task_id="landing_to_bronze",
        python_callable=run_script,
        op_args=["landing_to_bronze.py"],
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_script,
        op_args=["bronze_to_silver.py"],
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_script,
        op_args=["silver_to_gold.py"],
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
