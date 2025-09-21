import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, 0, 0),
}

with DAG(
    dag_id="fp_batch_datalake_zyaremko",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["zyaremko"],
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=os.path.join(BASE_PATH, "landing_to_bronze.py"),
        conn_id="spark-default",
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=os.path.join(BASE_PATH, "bronze_to_silver.py"),
        conn_id="spark-default",
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=os.path.join(BASE_PATH, "silver_to_gold.py"),
        conn_id="spark-default",
        verbose=1,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold





