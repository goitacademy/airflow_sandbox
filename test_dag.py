from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'test_dummy_dag_tutorial',
    default_args=default_args,
    description='Тестовий DAG, який нічого не робить',
    schedule_interval=None,
    start_date=datetime(2024, 11, 28),
    catchup=False,
    tags=['tutorial'],
) as dag:

    start_task = DummyOperator(
        task_id='start_task'
    )

    end_task = DummyOperator(
        task_id='end_task'
    )

    start_task >> end_task

