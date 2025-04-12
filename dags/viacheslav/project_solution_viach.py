from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import logging
import os

# Get current directory to construct relative paths
current_dir = os.getcwd()

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Logger for this DAG
log = logging.getLogger(__name__)

# Define the DAG
dag = DAG(
    dag_id='athlete_data_pipeline_viach',
    default_args=default_args,
    description='DAG: landing → bronze → silver → gold with Spark',
    schedule_interval=timedelta(days=1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=['spark', 'pipeline', 'athlete'],
)

# Spark script paths
spark_scripts = {
    'landing_to_bronze': os.path.join('/opt/airflow/dags/viacheslav', 'landing_to_bronze_viach.py'),
    'bronze_to_silver': os.path.join('/opt/airflow/dags/viacheslav', 'bronze_to_silver_viach.py'),
    'silver_to_gold': os.path.join('/opt/airflow/dags/viacheslav', 'silver_to_gold_viach.py'),
}

# Define tasks
landing_to_bronze = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application=spark_scripts['landing_to_bronze'],
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],  # Optional: add input/output path args
    executor_memory='2G',
    driver_memory='1G',
)

bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application=spark_scripts['bronze_to_silver'],
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    executor_memory='4G',
    driver_memory='1G',
)

silver_to_gold = SparkSubmitOperator(
    task_id='silver_to_gold',
    application=spark_scripts['silver_to_gold'],
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    executor_memory='4G',
    driver_memory='2G',
)

# Set dependencies
landing_to_bronze >> bronze_to_silver >> silver_to_gold

# Log on DAG load
log.info("✅ DAG 'athlete_data_pipeline_viach' is loaded and ready.")