from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

# Default options for DAG
default_args = { 
    'owner': 'liudmyla', 
    'depends_on_past': False, 
    'start_date': datetime(2024, 11, 1), 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG
dag = DAG( 
    'olympic_data_lake_pipeline', 
    default_args=default_args, 
    description='Multi-hop data lake ETL pipeline for Olympic athlete data', 
    schedule_interval=timedelta(days=1), # Daily run
    catchup=False,
    tags=['data-lake', 'etl', 'spark', 'olympic']
)

def print_start():
    """Function to log the start of the pipeline"""
    print("=" * 80)
    print("START OF OLYMPIC DATA LAKE PIPELINE")
    print("=" * 80)
    print(f"Timestamp: {datetime.now()}")
    print(f"Pipeline: Landing → Bronze → Silver → Gold")
    print("=" * 80)

def print_end():
    """Function to log the end of the pipeline"""
    print("=" * 80)
    print("COMPLETION OF OLYMPIC DATA LAKE PIPELINE")
    print("=" * 80)
    print(f"Timestamp: {datetime.now()}")
    print("Status: SUCCESS")
    print("=" * 80)

# Task 0: Start of the pipeline (optional, for logging)
start_task = PythonOperator(
    task_id='pipeline_start',
    python_callable=print_start,
    dag=dag
)

# Task 1: Landing -> Bronze
# Upload CSV from FTP and convert to Parquet
landing_to_bronze_task = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='dags/liudmyla/landing_to_bronze.py', # Change to your path
    conn_id='spark-default',
    verbose=True,
    dag=dag
)

# Task 2: Bronze → Silver
# Text cleaning and deduplication
bronze_to_silver_task = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='dags/liudmyla/bronze_to_silver.py', # Change to your path
    conn_id='spark-default',
    verbose=True,
    dag=dag
)

# Task 3: Silver → Gold
# Table JOIN and aggregation
silver_to_gold_task = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='dags/liudmyla/silver_to_gold.py', # Change to your path
    conn_id='spark-default',
    verbose=True,
    dag=dag
)

# Task 4: End pipeline (optional, for logging)
end_task = PythonOperator(
    task_id='pipeline_end',
    python_callable=print_end,
    dag=dag
)

start_task >> landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> end_task
