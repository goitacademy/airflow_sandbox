"""
Канарковий DAG — мінімальна перевірка, що pipeline деплоїть файли у
правильне місце в Airflow. Структура точно скопійована зі стилю робочих
студентських DAG-ів (goit_de_hw_07_pavlyshyn тощо).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def _hello():
    print("Hello from canary_zhuk_liudmyla!")


with DAG(
    dag_id="canary_zhuk_liudmyla",
    start_date=datetime(2024, 8, 4),
    schedule_interval=None,
    catchup=False,
    tags=["zhuk_liudmyla"],
) as dag:
    PythonOperator(task_id="hello", python_callable=_hello)
