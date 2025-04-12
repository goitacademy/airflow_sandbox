from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'athlete_data_pipeline_viach',
    default_args=default_args,
    description='Data pipeline for athlete data processing',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define tasks using SparkSubmitOperator
landing_to_bronze_task = SparkSubmitOperator(
    application='viacheslav/landing_to_bronze_viach.py',
    task_id='landing_to_bronze',
    conn_id='spark-default',  # Using the default Spark connection
    verbose=1,
    dag=dag,
)

bronze_to_silver_task = SparkSubmitOperator(
    application='viacheslav/bronze_to_silver_viach.py',
    task_id='bronze_to_silver',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

silver_to_gold_task = SparkSubmitOperator(
    application='viacheslav/silver_to_gold_viach.py', 
    task_id='silver_to_gold',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
)

# Set task dependencies
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task
