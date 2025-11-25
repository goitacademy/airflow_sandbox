import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="maryna_kindras_fp",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="maryna_kindras_landing_to_bronze",
        application="/usr/local/spark/app/maryna_kindras_landing_to_bronze.py",
        conn_id="spark-default",
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="maryna_kindras_bronze_to_silver",
        application="/usr/local/spark/app/maryna_kindras_bronze_to_silver.py",
        conn_id="spark-default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="maryna_kindras_silver_to_gold",
        application="/usr/local/spark/app/maryna_kindras_silver_to_gold.py",
        conn_id="spark-default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
