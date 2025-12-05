from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import os

# Базова директорія де лежить цей файл і всі Spark-скрипти
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LANDING_TO_BRONZE_APP = os.path.join(BASE_DIR, "oie_landing_to_bronze.py")
BRONZE_TO_SILVER_APP = os.path.join(BASE_DIR, "oie_bronze_to_silver.py")
SILVER_TO_GOLD_APP = os.path.join(BASE_DIR, "oie_silver_to_gold.py")


# Визначаємо налаштування за замовчуванням
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Оголошуємо DAG
with DAG(
    dag_id='oie_final_project_etl_dag',
    default_args=default_args,
    description='ETL Pipeline: Landing -> Bronze -> Silver -> Gold',
    schedule_interval=None,
    catchup=False,
    tags=['oie', 'spark', 'etl'],
) as dag:

    # Landing to Bronze
    # Запускає скрипт завантаження даних з FTP у Bronze (Parquet)
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        conn_id='spark-default',
        application=LANDING_TO_BRONZE_APP,
        verbose=True,
        dag=dag,
    )

    # Bronze to Silver
    # Запускає скрипт очищення тексту та дедублікації
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        conn_id='spark-default',
        application=BRONZE_TO_SILVER_APP,
        verbose=True,
        dag=dag,
    )

    # Silver to Gold
    # Запускає скрипт агрегації
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        conn_id='spark-default',
        application=SILVER_TO_GOLD_APP,
        verbose=True,
        dag=dag,
    )

    # Встановлення послідовності виконання
    # Спочатку Landing, потім Bronze, потім Gold
    landing_to_bronze >> bronze_to_silver >> silver_to_gold