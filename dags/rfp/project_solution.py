import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LANDING_TO_BRONZE_APP = os.path.join(BASE_DIR, "landing_to_bronze.py")
BRONZE_TO_SILVER_APP = os.path.join(BASE_DIR, "bronze_to_silver.py")
SILVER_TO_GOLD_APP = os.path.join(BASE_DIR, "silver_to_gold.py")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 1),
}

with DAG(
    dag_id="romans_fin_proj_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["romans"],
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=LANDING_TO_BRONZE_APP,
        conn_id="spark-default",
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=BRONZE_TO_SILVER_APP,
        conn_id="spark-default",
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=SILVER_TO_GOLD_APP,
        conn_id="spark-default",
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
