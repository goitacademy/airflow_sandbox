import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Базові аргументи DAG
default_args = {
    'owner': 'zyaremko',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Поточна директорія (щоб працювали відносні шляхи)
current_directory = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id='zyaremko_final_part2_pipeline',   # 👈 твій унікальний DAG ID
    default_args=default_args,
    description='Landing -> Bronze -> Silver -> Gold Spark jobs (Part 2)',
    schedule_interval=None,                   # ручний запуск
    start_date=datetime(2025, 9, 22),         # актуальна дата старту
    catchup=False,
    max_active_runs=1,
    tags=["zyaremko", "final_project"]        # 👈 щоб легко знайти у Airflow
) as dag:

    # Завдання 1: Landing -> Bronze
    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(current_directory, 'landing_to_bronze.py'),
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1
    )

    # Завдання 2: Bronze -> Silver
    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(current_directory, 'bronze_to_silver.py'),
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1
    )

    # Завдання 3: Silver -> Gold
    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(current_directory, 'silver_to_gold.py'),
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1
    )

    # Встановлення послідовності
    landing_to_bronze >> bronze_to_silver >> silver_to_gold

