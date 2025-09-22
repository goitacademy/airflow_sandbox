from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 21),
    "retries": 1,
}

with DAG(
    dag_id="project_solution_streaming",
    default_args=default_args,
    description="Final Project — Streaming Pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["final_project", "streaming"],
) as dag:

    streaming_pipeline = SparkSubmitOperator(
        task_id="streaming_pipeline",
        conn_id="spark-default",
        application="zyaremko_final_fp/streaming_pipeline.py",  # ✅ правильний шлях
        verbose=True,
        name="arrow-spark",
        conf={
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g"
        },
    )

    streaming_pipeline

