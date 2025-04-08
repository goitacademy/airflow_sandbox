from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

# Connection ID for Spark
connection_id = 'spark-default'

# Define DAG
with DAG(
        'FP_Sergii_S',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["Sergii_S"]
) as dag:

    # Define the full path to the scripts in the GitHub folder
    landing_to_bronze_path = '/opt/airflow/airflow_sandbox/Sergii_S/landing_to_bronze.py'
    bronze_to_silver_path = '/opt/airflow/airflow_sandbox/Sergii_S/bronze_to_silver.py'
    silver_to_gold_path = '/opt/airflow/airflow_sandbox/Sergii_S/silver_to_gold.py'

    # Define SparkSubmitOperator tasks with full paths to the scripts
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application=landing_to_bronze_path,  # Full path
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=bronze_to_silver_path,  # Full path
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=silver_to_gold_path,  # Full path
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    # Define task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold

