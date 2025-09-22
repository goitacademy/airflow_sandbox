from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Аргументи DAG
default_args = {
    "owner": "zoryana",
    "start_date": datetime(2025, 9, 22),
    "retries": 1,
}

# Визначення DAG
with DAG(
    dag_id="zoryana_streaming_fp",
    default_args=default_args,
    schedule_interval=None,  # запускати вручну
    catchup=False,
    tags=["zoryana", "final_project", "streaming"]
) as dag:

    # Spark job для запуску стрімінгового пайплайну
    run_streaming = SparkSubmitOperator(
        task_id="run_streaming_pipeline",
        application="spark_jobs/streaming_pipeline.py",  # шлях до spark-скрипта
        conn_id="spark-default",
        verbose=1
    )

    run_streaming
