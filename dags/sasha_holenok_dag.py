from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.sensors.mysql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import random
import time


TABLE_NAME = 'medal_counts'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 13),
}

def pick_medal():
    return random.choice(['Bronze', 'Silver', 'Gold'])

def pick_branch(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')
    return f'calc_{medal}'

def delay():
    time.sleep(5)

with DAG(
    'sasha_holenok_homework_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['sasha_holenok']
) as dag:


    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='sasha_holenok_db',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal
    )


    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_branch
    )


    tasks_to_branch = []
    for medal in ['Bronze', 'Silver', 'Gold']:
        calc_task = MySqlOperator(
            task_id=f'calc_{medal}',
            mysql_conn_id='sasha_holenok_db',
            sql=f"""
            INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = '{medal}';
            """
        )
        tasks_to_branch.append(calc_task)


    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=delay,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )


    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        mysql_conn_id='sasha_holenok_db',
        sql=f"SELECT 1 FROM {TABLE_NAME} WHERE created_at >= NOW() - INTERVAL 30 SECOND LIMIT 1;",
        poke_interval=5,
        timeout=60
    )


    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> tasks_to_branch
    tasks_to_branch >> generate_delay >> check_for_correctness