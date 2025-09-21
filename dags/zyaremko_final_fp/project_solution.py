import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# базовий шлях до цієї папки
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fp_batch_datalake_zyaremko",
    default_args=default_args,
    description="Final Project: Landing -> Bronze -> Silver -> Gold",
    schedule_interval=None,   # запускати вручну
    start_date=datetime(2025, 9, 20),
    catchup=False,
    max_active_runs=1,
    tags=["zyaremko"],
) as dag:

    # --- Landing -> Bronze ---
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=os.path.join(BASE_PATH, "landing_to_bronze.py"),
        conn_id="spark-default",
        verbose=1,
    )

    # --- Bronze -> Silver ---
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=os.path.join(BASE_PATH, "bronze_to_silver.py"),
        conn_id="spark-default",
        verbose=1,
    )

    # --- Silver -> Gold ---
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=os.path.join(BASE_PATH, "silver_to_gold.py"),
        conn_id="spark-default",
        verbose=1,
    )

    # порядок виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold




