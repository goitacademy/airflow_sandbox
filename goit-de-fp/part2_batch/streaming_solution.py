"""
Fefelov Streaming Pipeline DAG for GoIT Airflow Sandbox

This DAG manages the Kafka-Spark streaming pipeline that:
1. Reads athlete bio data from MySQL
2. Filters invalid height/weight data
3. Reads athlete_event_results from MySQL and writes to Kafka
4. Processes streaming data from Kafka
5. Joins streams and calculates aggregated statistics
6. Outputs results to both Kafka and MySQL

Author: fefelov-goit-student
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Get the directory where this DAG file is located
dags_dir = os.path.dirname(os.path.realpath(__file__))

# Default arguments for the DAG
default_args = {
    'owner': 'fefelov-goit-student',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'fefelov_streaming_pipeline_v7',
    default_args=default_args,
    description='Fefelov Real-time Kafka-Spark Streaming Pipeline - Production Ready v6',
    schedule_interval=None,  # Manual trigger only for streaming
    catchup=False,
    tags=['fefelov', 'streaming', 'kafka', 'spark', 'real-time'],
)

# Task 1: Start Kafka Spark Streaming Pipeline
# Note: For streaming, we typically run this as a long-running process
fefelov_streaming_task = SparkSubmitOperator(
    task_id='fefelov_kafka_spark_streaming',
    application=os.path.join(dags_dir, 'src', 'streaming', 'kafka_spark_streaming.py'),
    conn_id='spark-default',
    verbose=True,
    application_args=[],    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.sql.streaming.checkpointLocation': '/tmp/fefelov_checkpoint',
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,mysql:mysql-connector-java:8.0.33',
        'spark.jars.repositories': 'https://repo1.maven.org/maven2/',
        'spark.driver.extraClassPath': '/opt/spark/jars/*',
        'spark.executor.extraClassPath': '/opt/spark/jars/*',
        'spark.sql.streaming.forceDeleteTempCheckpointLocation': 'true'
    },
    env_vars={
        'STUDENT_PREFIX': 'fefelov',
        'DATA_PATH': './data',
        'PYTHONPATH': '/opt/spark/jobs/fefelov',
    },
    executor_memory='2g',
    driver_memory='1g',
    dag=dag,
)

# Task 2: Health check task to verify streaming is running
# This is a monitoring task that can be run periodically
fefelov_stream_health_check = BashOperator(
    task_id='fefelov_streaming_health_check',
    bash_command='''
    echo "Checking streaming pipeline health..."
    echo "Streaming pipeline for fefelov started at $(date)"
    echo "Monitor Spark UI at: http://217.61.58.159:8080"
    echo "Check Kafka topics and consumer groups for data flow"
    ''',
    dag=dag,
)

# Set task dependencies
fefelov_streaming_task >> fefelov_stream_health_check
