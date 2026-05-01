from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
from datetime import datetime

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
}

current_directory = os.path.dirname(os.path.abspath(__file__))

landing_script = os.path.join(current_directory, "fp_2_landing_to_bronze.py")
bronze_script = os.path.join(current_directory, "fp_2_bronze_to_silver.py")
gold_script = os.path.join(current_directory, "fp_2_silver_to_gold.py")


with DAG(
    dag_id='fp_2_project_solution_ds',
    default_args=default_args,
    description='Landing -> Bronze -> Silver -> Gold',
    schedule=None,
    tags=['kostiya'],
    catchup=False,
)as dag:

    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(current_directory, "fp_2_landing_to_bronze.py"),
        task_id="landing_to_bronze",
        conn_id="spark_default",
        py_files=os.path.join(current_directory, "downloader.py"),
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(current_directory, "fp_2_bronze_to_silver.py"),
        task_id="bronze_to_silver",
        conn_id="spark_default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(current_directory, "fp_2_silver_to_gold.py"),
        task_id="silver_to_gold",
        conn_id="spark_default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold


