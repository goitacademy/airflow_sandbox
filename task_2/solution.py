from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

dags_dir = os.path.dirname(os.path.abspath(__file__))

# Визначення DAG
with DAG(
        'pinkie_pie_medalion_architecture_flow',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["new_hanna"]  # Теги для класифікації DAG
) as dag:


    landing_to_bronze = SparkSubmitOperator(
        task_id='LandingToBronze',
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        conn_id='spark-default',
        verbose=1
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='BronzeToSilver',
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        conn_id='spark-default',
        verbose=1
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='SilverToGold',
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        conn_id='spark-default',
        verbose=1
    )

    # Встановлення залежностей між завданнями
    landing_to_bronze >> bronze_to_silver >> silver_to_gold