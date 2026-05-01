from airflow import DAG
from airflow.operators.bash import BashOperator

import os
from datetime import datetime

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="fp_2_project_solution_ds",
    start_date=datetime(2024, 10, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["fp_2_project_solution"],
) as dag:

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'fp_2_landing_to_bronze.py')}",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'fp_2_bronze_to_silver.py')}",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'fp_2_silver_to_gold.py')}",
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold