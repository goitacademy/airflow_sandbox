from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from zoryana_yaremko_fp_2.step1_read_mysql import read_mysql
from zoryana_yaremko_fp_2.step2_read_results import read_results
from zoryana_yaremko_fp_2.step3_join_bio_results import join_and_aggregate

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='zyaremko_final_project_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["zyaremko_fp_2"]
) as dag:

    t1 = PythonOperator(
        task_id='read_mysql',
        python_callable=read_mysql
    )

    t2 = PythonOperator(
        task_id='read_results',
        python_callable=read_results
    )

    t3 = PythonOperator(
        task_id='join_bio_results',
        python_callable=join_bio_results
    )

    t1 >> t2 >> t3


