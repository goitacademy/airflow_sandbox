from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'athlete_data_pipeline_viach',
    default_args=default_args,
    description='Data pipeline for athlete data processing',
    schedule_interval=None,
    catchup=False,
)

landing_to_bronze_task = SparkSubmitOperator(
    application='dags/viacheslav/landing_to_bronze.py',
    task_id='landing_to_bronze',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

bronze_to_silver_task = SparkSubmitOperator(
    application='dags/viacheslav/bronze_to_silver.py',
    task_id='bronze_to_silver',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

silver_to_gold_task = SparkSubmitOperator(
    application='dags/viacheslav/silver_to_gold.py',
    task_id='silver_to_gold',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task