from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os  # <--- импорт os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'athlete_data_pipeline_viach',
    default_args=default_args,
    description='Data pipeline for athlete data processing',
    schedule_interval=None,
    catchup=False,
)

# Абсолютный путь к текущей директории DAG-файла
base_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))

common_spark_config = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "4",
}

def create_spark_task(task_id, script_name):
    return SparkSubmitOperator(
        task_id=task_id,
        application=os.path.join(base_script_path, script_name),
        conn_id='spark-default',
        verbose=True,
        conf=common_spark_config,
        env_vars={
            "PYSPARK_PYTHON": "python3",
            "LOG_LEVEL": "INFO"
        },
        dag=dag,
    )

# Define tasks
landing_to_bronze_task = create_spark_task('landing_to_bronze', 'landing_to_bronze.py')
bronze_to_silver_task = create_spark_task('bronze_to_silver', 'bronze_to_silver.py')
silver_to_gold_task = create_spark_task('silver_to_gold', 'silver_to_gold.py')

# Set dependencies
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task