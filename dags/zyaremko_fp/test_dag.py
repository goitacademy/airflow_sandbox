from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from Zoryana's DAG!")

default_args = {
    'owner': 'zoryana',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id="zoryana_test_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["zoryana"]
) as dag:
    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello
    )
