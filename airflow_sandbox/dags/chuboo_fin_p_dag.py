from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id='chuboo_fin_p',
    default_args=default_args,
    description='DAG to run Spark ETL pipeline for Data Lake',
    schedule_interval=None,  # On-demand
    catchup=False
)

# Task 1: Landing to Bronze
landing_to_bronze = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='dags/landing_to_bronze.py',
    conn_id='spark-default',
    verbose=True,
    dag=dag
)

# Task 2: Bronze to Silver
bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='dags/bronze_to_silver.py',
    conn_id='spark-default',
    verbose=True,
    dag=dag
)

# Task 3: Silver to Gold
silver_to_gold = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='dags/silver_to_gold.py',
    conn_id='spark-default',
    verbose=True,
    dag=dag
)

# Define DAG dependencies
landing_to_bronze >> bronze_to_silver >> silver_to_gold
