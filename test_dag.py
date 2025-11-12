from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def choose_medal():
    medals = ['Gold', 'Silver', 'Bronze']
    chosen = random.choice(medals)
    print(f"Обрано медаль: {chosen}")
    return chosen

def process_medal():
    print("Обробка медалі...")
    return "Processed"

with DAG(
    'test_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'medals']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    choose_medal_task = PythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal
    )
    
    process_medal_task = PythonOperator(
        task_id='process_medal',
        python_callable=process_medal
    )
    
    end = DummyOperator(task_id='end')
    
    start >> choose_medal_task >> process_medal_task >> end

print("✅ test_dag successfully loaded!")
