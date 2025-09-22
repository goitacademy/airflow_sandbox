from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'zoryana',
    'start_date': datetime(2025, 9, 22),
    'retries': 1,
}

with DAG(
    dag_id="zoryana_streaming_fp",
    default_args=default_args,
    schedule_interval=None,  # запускати вручну
    catchup=False,
    tags=["zoryana", "final_project", "streaming"]
) as dag:

    run_streaming_pipeline = SparkSubmitOperator(
        task_id="run_streaming_pipeline",
        application="zyaremko_fp/streaming_pipeline.py",  
        conn_id="spark-default",
        verbose=True
    )

    run_streaming_pipeline

