import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# -------------------- paths --------------------
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
# ------------------------------------------------


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        'romans_fin_proj_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["romans"]
) as dag:

    landing_to_bronze_task = SparkSubmitOperator(
        task_id="run_landing_to_bronze",
        application=f"{DAG_DIR}/landing_to_bronze.py",
        conn_id="spark-default",
        verbose=1,
    )

    bronze_to_silver_task = SparkSubmitOperator(
        task_id="run_bronze_to_silver",
        application=f"{DAG_DIR}/bronze_to_silver.py",
        conn_id="spark-default",
        verbose=1,
    )

    silver_to_gold_task = SparkSubmitOperator(
        task_id="run_silver_to_gold",
        application=f"{DAG_DIR}/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
    )

    landing_to_bronze_task >> bronze_to_silver_task
    bronze_to_silver_task >> silver_to_gold_task
