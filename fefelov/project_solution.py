"""
Airflow DAG for Batch Data Lake Pipeline
Part 2, Step 4 of the Final Project
Updated for GoIT External Services with fefelov prefix
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Get the directory where this DAG file is located
dags_dir = os.path.dirname(os.path.realpath(__file__))

# Default arguments for the DAG
default_args = {
    'owner': 'fefelov-goit-student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG with prefixed name
dag = DAG(
    'fefelov_batch_pipeline-4',
    default_args=default_args,
    description='GoIT DE Final Project - Batch Data Lake Pipeline (fefelov)',
    schedule_interval='@daily',  # Run daily
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one DAG run at a time
    tags=['goit', 'data-engineering', 'final-project', 'batch', 'fefelov'],
)

# Task 1: Landing to Bronze
landing_to_bronze_task = SparkSubmitOperator(
    task_id='fefelov_landing_to_bronze',
    application=os.path.join(dags_dir, 'src', 'batch', 'landing_to_bronze.py'),
    conn_id='spark-default',
    verbose=True,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    },
    env_vars={
        'STUDENT_PREFIX': 'fefelov',
        'DATA_PATH': './data',
        'PYTHONPATH': '/opt/spark/jobs/src',
    },
    dag=dag,
)

# Task 2: Bronze to Silver
bronze_to_silver_task = SparkSubmitOperator(
    task_id='fefelov_bronze_to_silver',
    application=os.path.join(dags_dir, 'src', 'batch', 'bronze_to_silver.py'),
    conn_id='spark-default',
    verbose=True,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    },
    env_vars={
        'STUDENT_PREFIX': 'fefelov',
        'DATA_PATH': './data',
        'PYTHONPATH': '/opt/spark/jobs/src',
    },
    dag=dag,
)

# Task 3: Silver to Gold
silver_to_gold_task = SparkSubmitOperator(
    task_id='fefelov_silver_to_gold',
    application=os.path.join(dags_dir, 'src', 'batch', 'silver_to_gold.py'),
    conn_id='spark-default',
    verbose=True,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    },
    env_vars={
        'STUDENT_PREFIX': 'fefelov',
        'DATA_PATH': './data',
        'PYTHONPATH': '/opt/spark/jobs/src',
    },
    dag=dag,
)

# Define task dependencies
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task

# Task documentation
landing_to_bronze_task.doc_md = """
## Landing to Bronze Task

This task downloads CSV files from the FTP server and converts them to Parquet format in the bronze layer.

### What it does:
1. Downloads athlete_bio.csv and athlete_event_results.csv from FTP
2. Reads CSV files with Spark
3. Saves data as Parquet files in the bronze layer

### Input: FTP CSV files
### Output: Bronze layer Parquet files
"""

bronze_to_silver_task.doc_md = """
## Bronze to Silver Task

This task cleans and deduplicates data from the bronze layer.

### What it does:
1. Reads Parquet files from bronze layer
2. Cleans text columns using regex
3. Removes duplicate rows
4. Saves cleaned data to silver layer

### Input: Bronze layer Parquet files
### Output: Silver layer Parquet files
"""

silver_to_gold_task.doc_md = """
## Silver to Gold Task

This task joins data and creates the final analytical dataset.

### What it does:
1. Reads athlete_bio and athlete_event_results from silver layer
2. Joins data on athlete_id
3. Calculates average weight and height by sport, medal, sex, country
4. Adds timestamp and saves to gold layer

### Input: Silver layer Parquet files
### Output: Gold layer analytical dataset
"""

dag.doc_md = """
# GoIT Data Engineering Final Project - Batch Pipeline (fefelov)

This DAG implements a multi-hop data lake architecture with three layers for student fefelov.

## Architecture:
- **Landing**: Raw CSV files from FTP
- **Bronze**: Raw data in Parquet format
- **Silver**: Cleaned and deduplicated data  
- **Gold**: Analytical datasets ready for consumption

## Pipeline Flow:
1. **Landing → Bronze**: Download and convert CSV to Parquet
2. **Bronze → Silver**: Clean and deduplicate data
3. **Silver → Gold**: Join and aggregate for analytics

## Data Sources:
- athlete_bio.csv: Athlete biographical information
- athlete_event_results.csv: Athletic event results

## Final Output:
Aggregated statistics showing average height and weight by:
- Sport
- Medal type
- Gender
- Country

## Multi-User Environment:
All resources (tables, topics, DAGs) are prefixed with "fefelov" to avoid conflicts 
with other students on the shared GoIT cloud infrastructure.
"""
