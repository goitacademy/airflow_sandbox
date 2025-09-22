from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Визначення DAG
with DAG(
        'project_solution_streaming',   # ✅ унікальна назва DAG
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        tags=["zyaremko"]               # ✅ твій тег
) as dag:
    streaming_pipeline = SparkSubmitOperator(
        application='dags/zyaremko_final_fp/streaming_pipeline.py',  # ✅ правильний шлях
        task_id='streaming_pipeline',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )



