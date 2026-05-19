from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
 
# Final Project Part 2 — Airflow DAG
# Runs landing→bronze→silver→gold pipeline using SparkSubmitOperator
 
default_args = {
    "owner": "ira",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}
 
with DAG(
    dag_id="fp_datalake_pipeline_ira",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["final_project", "part2"],
) as dag:
 
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="dags/ira/landing_to_bronze.py",
        conn_id="spark-default",
        verbose=1,
    )
 
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="dags/ira/bronze_to_silver.py",
        conn_id="spark-default",
        verbose=1,
    )
 
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="dags/ira/silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
    )
 
    landing_to_bronze >> bronze_to_silver >> silver_to_gold