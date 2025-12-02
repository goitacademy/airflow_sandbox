import os
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


# ВИПРАВЛЕНО: файли знаходяться в /opt/airflow/dags/goit-de-fp-main/dags/
BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/dags/goit-de-fp-main/dags")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

path = Path(__file__).parent
with DAG(
        'k0dima_final_project',
        default_args=default_args,
        schedule=None,
        catchup=False,
        tags=["k0dima"]
) as dag:
      
    landing_to_bronze = SparkSubmitOperator(
        application=path / 'landing_to_bronze.py',
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )
    
    bronze_to_silver = SparkSubmitOperator(
        application=path / 'bronze_to_silver.py',
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )
    
        
    silver_to_gold = SparkSubmitOperator(
        application=path / 'silver_to_gold.py',
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )
    
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
    