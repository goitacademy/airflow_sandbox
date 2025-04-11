from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 11),
    'retries' : 1
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

load_bronze = SparkSubmitOperator(
    application='dags/landing_to_bronze.py',
    task_id='load_bronze',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

clean_silver = SparkSubmitOperator(
    application='dags/bronze_to_silver.py',
    task_id='clean_silver',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

calculate_gold = SparkSubmitOperator(
    application='dags/silver_to_gold.py',
    task_id='calculate_gold',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

load_bronze >> clean_silver >> calculate_gold
