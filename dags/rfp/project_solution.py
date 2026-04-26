import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from landing_to_bronze import main as landing_to_bronze
from bronze_to_silver import main as bronze_to_silver
from silver_to_gold import main as silver_to_gold

# -------------------- paths --------------------
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
# ------------------------------------------------


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        'romans_fin_proj_dag',
        default_args=default_args,
        schedule_interval='*/10 * * * *',
        catchup=False,
        tags=["romans"]
) as dag:

    landing_to_bronze_task = PythonOperator(
        task_id='landing_to_bronze',
        python_callable=landing_to_bronze,
    )

    bronze_to_silver_task = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=bronze_to_silver,
    )

    silver_to_gold_task = PythonOperator(
        task_id='silver_to_gold',
        python_callable=silver_to_gold,
    )

    landing_to_bronze_task >> bronze_to_silver_task
    bronze_to_silver_task >> silver_to_gold_task
