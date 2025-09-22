from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        dag_id="project_solution_streaming",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["zyaremko"]
) as dag:

    streaming_task = SparkSubmitOperator(
        task_id="streaming_pipeline",
        conn_id="spark-default",
        application="/opt/airflow/dags/zyaremko_final_fp/streaming_pipeline.py",
        verbose=True,
    )



