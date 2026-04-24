"""
Part 2 · project_solution
=========================

Airflow DAG, що запускає послідовно три Spark-jobs:

    landing_to_bronze  →  bronze_to_silver  →  silver_to_gold

Файли мають лежати у папці airflow_sandbox/dags/zhuk_liudmyla_fp/
разом з цим DAG-файлом. Шляхи до application-ів у SparkSubmitOperator
вказані відносно кореня airflow_sandbox.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

STUDENT = "zhuk_liudmyla"
APP_BASE = f"dags/{STUDENT}_fp"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4),
}

with DAG(
    dag_id=f"{STUDENT}_fp_batch_datalake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=[STUDENT, "final_project", "part2"],
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=f"{APP_BASE}/landing_to_bronze.py",
        conn_id="spark-default",
        verbose=1,
        dag=dag,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{APP_BASE}/bronze_to_silver.py",
        conn_id="spark-default",
        verbose=1,
        dag=dag,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{APP_BASE}/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
        dag=dag,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
