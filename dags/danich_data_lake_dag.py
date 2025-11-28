from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.dag import DAG
from datetime import datetime

# Налаштування DAG
with DAG(
    dag_id="danich_multi_hop_data_lake_etl",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["danich", "spark", "data_lake", "final_project"],
) as dag:
    # Шляхи до Spark скриптів (відносно кореня репозиторію)
    BASE_DIR = "dags/danich_fp/"
    LANDING_TO_BRONZE_APP = f"{BASE_DIR}landing_to_bronze.py"
    BRONZE_TO_SILVER_APP = f"{BASE_DIR}bronze_to_silver.py"
    SILVER_TO_GOLD_APP = f"{BASE_DIR}silver_to_gold.py"

    # Завдання 1: Landing до Bronze (Завантаження + CSV до Parquet)
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=LANDING_TO_BRONZE_APP,
        conn_id="spark-default",
        verbose=1,
    )

    # Завдання 2: Bronze до Silver (Очищення + Дедуплікація)
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=BRONZE_TO_SILVER_APP,
        conn_id="spark-default",
        verbose=1,
    )

    # Завдання 3: Silver до Gold (Об'єднання + Агрегація)
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=SILVER_TO_GOLD_APP,
        conn_id="spark-default",
        verbose=1,
    )

    # Визначення порядку виконання завдань
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
