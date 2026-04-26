import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2026, 4, 26, tz="UTC"),
    "catchup": False,
}

DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))

with DAG(
    dag_id="sergiy_pavlyshyn_datalake_pipeline",
    default_args=default_args,
    schedule_interval=None,
    tags=["sergiy_pavlyshyn", "final_project"],
) as dag:
    task_landing_to_bronze = SparkSubmitOperator(
        task_id="run_landing_to_bronze",
        application=f"{DAG_FOLDER}/landing_to_bronze.py",  # Тепер шлях завжди буде точним
        conn_id="spark-default",
        verbose=1,
    )

    task_bronze_to_silver = SparkSubmitOperator(
        task_id="run_bronze_to_silver",
        application=f"{DAG_FOLDER}/bronze_to_silver.py",
        conn_id="spark-default",
        verbose=1,
    )

    task_silver_to_gold = SparkSubmitOperator(
        task_id="run_silver_to_gold",
        application=f"{DAG_FOLDER}/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
    )

    task_landing_to_bronze >> task_bronze_to_silver >> task_silver_to_gold
