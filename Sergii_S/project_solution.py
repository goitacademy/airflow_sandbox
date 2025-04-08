from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

# Connection ID for Spark
connection_id = 'spark-default'

# Define the base path for the scripts
base_path = '/Users/HP/Desktop/DATA_Eng/airflow_sandbox/Sergii_S'

# Define DAG
with DAG(
        'FP_Sergii_S',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["Sergii_S"]
) as dag:

    # Define SparkSubmitOperator tasks with verbose=1 and dag=dag
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application=f'{base_path}/landing_to_bronze.py',
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=f'{base_path}/bronze_to_silver.py',
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=f'{base_path}/silver_to_gold.py',
        conn_id=connection_id,
        verbose=1,
        dag=dag,  # Associate the task with the current DAG
    )

    # Define task dependencies
    landing_to_bronze >> bronze_to_silver >> silver_to_gold

