"""
Фінальний проєкт — Частина 2, Крок 4.
Airflow DAG: project_solution.

Послідовно запускає три Spark jobs:
  landing_to_bronze → bronze_to_silver → silver_to_gold
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Визначаємо поточну директорію, де лежить DAG та інші скрипти (пакетна структура)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="maxim_fp_datalake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["final_project", "datalake", "spark", "maxim"],
    description="Multi-hop Data Lake: landing → bronze → silver → gold",
) as dag:

    # Крок 1: Landing to Bronze
    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'maxim_landing_to_bronze.py')}",
    )

    # Крок 2: Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'maxim_bronze_to_silver.py')}",
    )

    # Крок 3: Silver to Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"spark-submit {os.path.join(DAG_DIR, 'maxim_silver_to_gold.py')}",
    )

    # Послідовне виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
