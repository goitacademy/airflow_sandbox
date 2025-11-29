from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os


# Базова директорія де лежить цей файл і всі Spark-скрипти
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

landing_script = os.path.join(BASE_DIR, "landing_to_bronze.py")
bronze_script = os.path.join(BASE_DIR, "bronze_to_silver.py")
gold_script = os.path.join(BASE_DIR, "silver_to_gold.py")



# Налаштування DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

dag = DAG(
    dag_id="fp_matvieienko_dag",
    default_args=default_args,
    description="Final project Batch Data Lake DAG by fp_matvieienko",
    schedule_interval=None,   # запуск вручну
    catchup=False,
    tags=["fp_matvieienko", "final_project"],
)



# Таски DAG

# 1. landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id="landing_to_bronze",
    bash_command=f"python {landing_script}",
    dag=dag,
)

# 2. bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=f"python {bronze_script}",
    dag=dag,
)

# 3. silver_to_gold.py
silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command=f"python {gold_script}",
    dag=dag,
)


# Послідовність виконання
landing_to_bronze >> bronze_to_silver >> silver_to_gold
