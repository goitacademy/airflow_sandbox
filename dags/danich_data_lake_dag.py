from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Базова директорія для DAG-файлів (та сама папка, де лежить цей файл)
DAGS_DIR = Path(__file__).parent

# Шляхи до Spark-скриптів (рахуються від поточного файлу, а не від /opt/...)
LANDING_TO_BRONZE_APP = str(DAGS_DIR / "danich_fp" / "landing_to_bronze.py")
BRONZE_TO_SILVER_APP = str(DAGS_DIR / "danich_fp" / "bronze_to_silver.py")
SILVER_TO_GOLD_APP = str(DAGS_DIR / "danich_fp" / "silver_to_gold.py")

# Налаштування DAG
with DAG(
    dag_id="danich_multi_hop_data_lake_etl",
    schedule=None,  # DAG запускається тільки вручну
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["danich", "spark", "data_lake", "final_project"],
) as dag:

    # Завдання 1: Landing -> Bronze (завантаження + CSV -> Parquet)
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        conn_id="spark-default",
        application=LANDING_TO_BRONZE_APP,
        verbose=True,
    )

    # Завдання 2: Bronze -> Silver (очищення + дедуплікація)
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark-default",
        application=BRONZE_TO_SILVER_APP,
        verbose=True,
    )

    # Завдання 3: Silver -> Gold (джойни + агрегація)
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="spark-default",
        application=SILVER_TO_GOLD_APP,
        verbose=True,
    )

    # Послідовність виконання задач
    landing_to_bronze >> bronze_to_silver >> silver_to_gold