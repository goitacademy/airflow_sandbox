import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 10),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="ola_de_fp2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL pipeline from landing to gold using Spark and Airflow",
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="ola_landing_to_bronze",
        application=os.path.join(BASE_DIR, "landing_to_bronze.py"),
        conn_id="spark-default",
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="ola_bronze_to_silver",
        application=os.path.join(BASE_DIR, "bronze_to_silver.py"),
        conn_id="spark-default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="ola_silver_to_gold",
        application=os.path.join(BASE_DIR, "silver_to_gold.py"),
        conn_id="spark-default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
