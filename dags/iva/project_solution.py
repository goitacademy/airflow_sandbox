import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

landing_script = os.path.join(BASE_DIR, "landing_to_bronze.py")
bronze_script = os.path.join(BASE_DIR, "bronze_to_silver.py")
gold_script = os.path.join(BASE_DIR, "silver_to_gold.py")

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="iva_batch_datalake",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    landing_bio = SparkSubmitOperator(
        task_id="landing_bio",
        application=landing_script,
        application_args=["athlete_bio"],
        conn_id="spark-default",
        verbose=1,
    )

    landing_results = SparkSubmitOperator(
        task_id="landing_results",
        application=landing_script,
        application_args=["athlete_event_results"],
        conn_id="spark-default",
        verbose=1,
    )

    bronze_bio = SparkSubmitOperator(
        task_id="bronze_bio",
        application=bronze_script,
        application_args=["athlete_bio"],
        conn_id="spark-default",
        verbose=1,
    )

    bronze_results = SparkSubmitOperator(
        task_id="bronze_results",
        application=bronze_script,
        application_args=["athlete_event_results"],
        conn_id="spark-default",
        verbose=1,
    )

    gold_layer = SparkSubmitOperator(
        task_id="gold_layer",
        application=gold_script,
        conn_id="spark-default",
        verbose=1,
    )

landing_bio >> landing_results >> [bronze_bio, bronze_results] >> gold_layer

