from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Визначення DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

dag = DAG(
    'Dmytro_de_fp_02',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["Dmytro"]
)

landing_to_bronze = SparkSubmitOperator(
    application='dags/Dmytro/landing_to_bronze.py',
    task_id='landing_to_bronze',
    conn_id='spark-default',
    verbose=True,
    dag=dag,
)

bronze_to_silver = SparkSubmitOperator(
    application='dags/Dmytro/bronze_to_silver.py',
    task_id='bronze_to_silver',
    conn_id='spark-default',
    verbose=True,
    dag=dag,
)

silver_to_gold = SparkSubmitOperator(
    application='dags/Dmytro/silver_to_gold.py',
    task_id='silver_to_gold',
    conn_id='spark-default',
    verbose=True,
    dag=dag,
)

# Встановлення залежностей
landing_to_bronze >> bronze_to_silver >> silver_to_gold
