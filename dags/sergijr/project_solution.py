
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dags_dir = os.path.dirname(os.path.realpath(__file__))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 12),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="rsmnys_batch_data_lake_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL pipeline from landing to gold using Spark and Airflow",
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="rsmnys_landing_to_bronze",
        application=os.path.join(dags_dir, "landing_to_bronze.py"),
        conn_id="spark-default",
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="rsmnys_bronze_to_silver",
        application=os.path.join(dags_dir, "bronze_to_silver.py"),
        conn_id="spark-default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="rsmnys_silver_to_gold",
        application=os.path.join(dags_dir, "silver_to_gold.py"),
        conn_id="spark-default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
