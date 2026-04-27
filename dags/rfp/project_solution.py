import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Визначаємо поточну директорію, де лежить DAG та інші скрипти (пакетна структура)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="romans_fin_proj_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["romans"],
) as dag:

    # Крок 1: Landing to Bronze
    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'landing_to_bronze.py')}",
    )

    # Крок 2: Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'bronze_to_silver.py')}",
    )

    # Крок 3: Silver to Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'silver_to_gold.py')}",
    )

    # Послідовне виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
