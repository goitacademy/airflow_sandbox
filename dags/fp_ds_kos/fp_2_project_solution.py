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
        application=landing_script,
        task_id='landing_to_bronze',
        conn_id='spark_default',
        verbose=1,
        dag=dag
    )

    bronze_to_silver = SparkSubmitOperator(
        application=bronze_script,
        task_id='bronze_to_silver',
        conn_id='spark_default',
        verbose=1,
        dag = dag
    )

    silver_to_gold = SparkSubmitOperator(
        application=gold_script,
        task_id='silver_to_gold',
        conn_id='spark_default',
        verbose=1,
        dag=dag
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold


