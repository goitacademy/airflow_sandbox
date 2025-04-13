from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import datetime

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 12, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_v_vasyliev"

my_tag = 'v_vasyliev'
dags_dir = os.path.dirname(os.path.abspath(__file__))


# Визначення DAG
with DAG(
        'VVV_FP_2',
        default_args=default_args,
        schedule_interval=None, # '*/10 * * * *',  # every 10 minutes
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=[f"{my_tag}"]  # Теги для класифікації DAG
) as my_dag:

    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1,
        dag=my_dag,
        )

    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1,
        dag=my_dag,
        )

    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1,
        dag=my_dag,
        )

    # Встановлення залежностей між завданнями
    landing_to_bronze >> bronze_to_silver >> silver_to_gold

