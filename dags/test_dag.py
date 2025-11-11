from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_task():
    print("Hello from test_dag by Serhii Kravchenko!")

with DAG(
    'test_dag_kravchenko',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['kravchenko', 'test']
) as dag:
    
    start = DummyOperator(task_id='start')
    hello = PythonOperator(
        task_id='hello_task',
        python_callable=hello_task
    )
    end = DummyOperator(task_id='end')
    
    start >> hello >> end

print("✅ test_dag_kravchenko loaded successfully!")
