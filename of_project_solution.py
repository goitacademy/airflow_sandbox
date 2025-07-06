from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 6, 1),
}

with DAG(
    dag_id='etl_datalake_pipeline',
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    landing_to_bronze = BashOperator(
        task_id='landing_to_bronze',
        bash_command='python3 landing_to_bronze.py'
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python3 bronze_to_silver.py'
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='python3 silver_to_gold.py'
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold