from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="project_solution_ilin",
    start_date=datetime(2025, 6, 14),
    schedule_interval=None,
    catchup=False
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="dags/ilin/landing_to_bronze.py",
        conn_id="spark-default",
        verbose=1
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="dags/ilin/bronze_to_silver.py",
        conn_id="spark-default",
        verbose=1
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="dags/ilin/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold