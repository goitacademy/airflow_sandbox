from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "iva",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="iva_batch_datalake",
    default_args=default_args,
    schedule_interval=None,   # запускаєш вручну
    catchup=False,
) as dag:

    # 1) Landing -> Bronze
    landing_bio = SparkSubmitOperator(
        task_id="landing_bio",
        application="dags/iva/landing_to_bronze.py",
        conn_id="spark-default",
        application_args=["athlete_bio"],
        verbose=1,
    )

    landing_results = SparkSubmitOperator(
        task_id="landing_results",
        application="dags/iva/landing_to_bronze.py",
        conn_id="spark-default",
        application_args=["athlete_event_results"],
        verbose=1,
    )

    # 2) Bronze -> Silver
    bronze_bio = SparkSubmitOperator(
        task_id="bronze_bio",
        application="dags/iva/bronze_to_silver.py",
        conn_id="spark-default",
        application_args=["athlete_bio"],
        verbose=1,
    )

    bronze_results = SparkSubmitOperator(
        task_id="bronze_results",
        application="dags/iva/bronze_to_silver.py",
        conn_id="spark-default",
        application_args=["athlete_event_results"],
        verbose=1,
    )

    # 3) Silver -> Gold
    gold = SparkSubmitOperator(
        task_id="gold_layer",
        application="dags/iva/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
    )

    # Залежності:
    landing_bio >> bronze_bio
    landing_results >> bronze_results
    [bronze_bio, bronze_results] >> gold

