from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'tsygankov',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='olympic_stream_dag',
    default_args=default_args,
    description='Запуск PySpark streaming job для Olympic dataset',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_spark_streaming = BashOperator(
        task_id='run_spark_streaming',
        bash_command=(
            "spark-submit "
            "--jars /path/to/mysql-connector-j-8.0.32.jar "
            "/path/to/your_spark_streaming_script.py"
        )
    )

    run_spark_streaming
