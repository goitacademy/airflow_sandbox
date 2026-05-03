"""Airflow DAG that orchestrates the Part 2 multi-hop datalake.

Five SparkSubmitOperator tasks run in sequence:

    landing_to_bronze (athlete_bio)
        -> landing_to_bronze (athlete_event_results)
            -> bronze_to_silver (athlete_bio)
                -> bronze_to_silver (athlete_event_results)
                    -> silver_to_gold

Each `application` path is relative to the Airflow `dags/` root, matching the
brief's example (`dags/oleksiy/spark_job.py`). Drop this file and the three
spark scripts under `dags/sergii_prostov_fp/` in the airflow_sandbox repo.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_FOLDER = "dags/sergii_prostov_fp"
SPARK_CONN_ID = "spark-default"

default_args = {
    "owner": "sergii_prostov",
    "start_date": datetime(2026, 5, 1),
}

with DAG(
    dag_id="sergii_prostov_datalake_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sergii_prostov", "final_project"],
) as dag:

    landing_to_bronze_bio = SparkSubmitOperator(
        task_id="landing_to_bronze_athlete_bio",
        application=f"{DAG_FOLDER}/landing_to_bronze.py",
        application_args=["athlete_bio"],
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    landing_to_bronze_results = SparkSubmitOperator(
        task_id="landing_to_bronze_athlete_event_results",
        application=f"{DAG_FOLDER}/landing_to_bronze.py",
        application_args=["athlete_event_results"],
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    bronze_to_silver_bio = SparkSubmitOperator(
        task_id="bronze_to_silver_athlete_bio",
        application=f"{DAG_FOLDER}/bronze_to_silver.py",
        application_args=["athlete_bio"],
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    bronze_to_silver_results = SparkSubmitOperator(
        task_id="bronze_to_silver_athlete_event_results",
        application=f"{DAG_FOLDER}/bronze_to_silver.py",
        application_args=["athlete_event_results"],
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{DAG_FOLDER}/silver_to_gold.py",
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    (
        landing_to_bronze_bio
        >> landing_to_bronze_results
        >> bronze_to_silver_bio
        >> bronze_to_silver_results
        >> silver_to_gold
    )
