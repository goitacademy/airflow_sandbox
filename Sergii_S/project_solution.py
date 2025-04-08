from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

# Connection ID for Spark
connection_id = 'spark-default'

# Define the DAG
with DAG(
        'FP_Sergii_S',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["Sergii_S"]
) as dag:

    # Define SparkSubmitOperator tasks with relative paths
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='Sergii_S/landing_to_bronze.py',  # relative path
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='Sergii_S/bronze_to_silver.py',  # relative path
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='Sergii_S/silver_to_gold.py',  # relative path
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    # Define task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold


