from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 6, 0, 0),
}

LANDING_TO_BRONZE_APP = os.path.join(BASE_DIR, "landing_to_bronze.py")
BRONZE_TO_SILVER_APP = os.path.join(BASE_DIR, "bronze_to_silver.py")
SILVER_TO_GOLD_APP = os.path.join(BASE_DIR, "silver_to_gold.py")

with DAG(
        'final_project_eli',
        default_args=default_args,
        schedule_interval='*/10 * * * *',
        catchup=False,
        tags=["final_project_eli"]
) as dag:
    landing_to_bronze_task = SparkSubmitOperator(
        application=LANDING_TO_BRONZE_APP,
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1,
        dag=dag
    )

    bronze_to_silver_task = SparkSubmitOperator(
        application=BRONZE_TO_SILVER_APP,
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1,
        dag=dag
    )

    silver_to_gold_task = SparkSubmitOperator(
        application=SILVER_TO_GOLD_APP,
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1,
        dag=dag
    )

# Setting dependencies
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task
