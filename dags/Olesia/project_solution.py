import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dags_dir = os.path.dirname(os.path.realpath(__file__))

default_args = {
    'start_date': datetime(2025, 5, 7),
    'owner': 'olesia',
}

with DAG(
    dag_id='final_project_etl_pipeline',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    description='ETL pipeline: landing -> bronze -> silver -> gold',
    tags=['final_project', 'goit']
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=os.path.join(dags_dir, "Olesia", "landing_to_bronze.py"),
        conn_id="spark-default",
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=os.path.join(dags_dir, "Olesia", "bronze_to_silver.py"),
        conn_id="spark-default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=os.path.join(dags_dir, "Olesia", "silver_to_gold.py"),
        conn_id="spark-default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
