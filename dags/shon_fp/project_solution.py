from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "shon",
    "start_date": datetime(2026, 4, 27),
    "retries": 1,
}


with DAG(
    dag_id="shon_fp_batch_datalake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["shon", "final_project", "spark", "datalake"],
) as dag:

    # Етап 1. Landing -> Bronze для athlete_bio
    landing_to_bronze_athlete_bio = SparkSubmitOperator(
        task_id="landing_to_bronze_athlete_bio",
        application="dags/shon_fp/landing_to_bronze.py",
        application_args=["athlete_bio"],
        conn_id="spark-default",
        verbose=1,
    )

    # Етап 1. Landing -> Bronze для athlete_event_results
    landing_to_bronze_athlete_event_results = SparkSubmitOperator(
        task_id="landing_to_bronze_athlete_event_results",
        application="dags/shon_fp/landing_to_bronze.py",
        application_args=["athlete_event_results"],
        conn_id="spark-default",
        verbose=1,
    )

    # Етап 2. Bronze -> Silver для athlete_bio
    bronze_to_silver_athlete_bio = SparkSubmitOperator(
        task_id="bronze_to_silver_athlete_bio",
        application="dags/shon_fp/bronze_to_silver.py",
        application_args=["athlete_bio"],
        conn_id="spark-default",
        verbose=1,
    )

    # Етап 2. Bronze -> Silver для athlete_event_results
    bronze_to_silver_athlete_event_results = SparkSubmitOperator(
        task_id="bronze_to_silver_athlete_event_results",
        application="dags/shon_fp/bronze_to_silver.py",
        application_args=["athlete_event_results"],
        conn_id="spark-default",
        verbose=1,
    )

    # Етап 3. Silver -> Gold avg_stats
    silver_to_gold_avg_stats = SparkSubmitOperator(
        task_id="silver_to_gold_avg_stats",
        application="dags/shon_fp/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
    )

    [
        landing_to_bronze_athlete_bio,
        landing_to_bronze_athlete_event_results,
    ] >> [
        bronze_to_silver_athlete_bio,
        bronze_to_silver_athlete_event_results,
    ] >> silver_to_gold_avg_stats