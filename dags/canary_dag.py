"""
Канарковий DAG — перевіряє, що файл з цього репо взагалі долітає до сервера
Airflow через спільний airflow_sandbox. Жодних провайдерів не використовує.

Якщо з'являється в Airflow UI → проблема не в пайплайні, а в імпортах/коді
нашого zhuk_liudmyla_medals_dag.py.
"""

from datetime import datetime

from airflow import DAG
# Використовуємо DummyOperator (легасі, але 100% є в будь-якій Airflow 2.x),
# як у відео-інструкції курсу. EmptyOperator — новіший, може не бути.
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, 0, 0),
}

with DAG(
    dag_id="canary_zhuk_liudmyla",
    description="Перевірка доставки DAG-ів у Airflow для Zhuk Liudmyla",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["canary", "zhuk_liudmyla"],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    start >> end
