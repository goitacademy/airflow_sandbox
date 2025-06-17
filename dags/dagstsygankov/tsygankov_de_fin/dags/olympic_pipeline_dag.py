from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='otsygankov_fp_dag',
    start_date=datetime(2025, 6, 1),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_pipeline',
        bash_command='bash run_spark_job.sh'
    )
