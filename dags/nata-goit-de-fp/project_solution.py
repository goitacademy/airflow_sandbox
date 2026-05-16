import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Визначаємо поточну директорію, де лежить DAG та інші скрипти
DAG_DIR = os.path.dirname(os.path.abspath(__file__))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 10),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="nata-goit-de-hw-final-project",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL pipeline from landing to gold using Spark and Airflow",
) as dag:

    # Крок 1: Landing to Bronze (використовуємо надійний BashOperator)
    landing_to_bronze = BashOperator(
        task_id="nata_landing_to_bronze",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'landing_to_bronze.py')}",
    )

    # Крок 2: Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id="nata_bronze_to_silver",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'bronze_to_silver.py')}",
    )

    # Крок 3: Silver to Gold
    silver_to_gold = BashOperator(
        task_id="nata_silver_to_gold",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'silver_to_gold.py')}",
    )

    # Послідовність виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold