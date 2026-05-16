import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 10),
    "depends_on_past": False,
    "retries": 1,
}

# Динамічно визначаємо шлях до папки dags на сервері
# Зазвичай це /opt/airflow/dags/назва_папки/скрипт.py
airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
base_path = os.path.join(airflow_home, "dags", "nata-goit-de-fp")

with DAG(
    dag_id="nata-goit-de-hw-final-project",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL pipeline from landing to gold using Spark and Airflow",
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="nata_landing_to_bronze",
        application=os.path.join(base_path, "landing_to_bronze.py"),
        conn_id="spark-default",
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="nata_bronze_to_silver",
        application=os.path.join(base_path, "bronze_to_silver.py"),
        conn_id="spark-default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="nata_silver_to_gold",
        application=os.path.join(base_path, "silver_to_gold.py"),
        conn_id="spark-default",
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold