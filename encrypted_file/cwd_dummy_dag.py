from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

def show_current_directory():
    cwd = os.getcwd()
    print(f"Current Working Directory: {cwd}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='debug_working_directory',
    default_args=default_args,
    schedule_interval=None,  # run on demand
    catchup=False,
    tags=['debug', 'utility']
) as dag:

    print_cwd = PythonOperator(
        task_id='print_current_working_directory',
        python_callable=show_current_directory
    )
