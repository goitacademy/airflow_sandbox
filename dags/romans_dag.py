from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime
import random
import time


def pick_medal():
    medal = random.choice(['Gold', 'Silver', 'Bronze'])
    print(f'Medal was selected: {medal}')
    return medal

def check_medal(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')

    if medal == 'Gold':
        return 'calc_gold_task'
    elif medal == 'Silver':
        return 'calc_silver_task'
    elif medal == 'Bronze':
        return 'calc_bronze_task'

def generate_delay():
    time.sleep(25)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 13)
}

connection_name = 'goit_mysql_db_romans'

with DAG(
    'medal_stats_romans',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['romans']
) as dag:
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql='''
        CREATE DATABASE IF NOT EXISTS romans;'''
    )

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql='''
        CREATE TABLE IF NOT EXISTS romans.medals (
        id int PRIMARY KEY AUTO_INCREMENT,
        medal_type VARCHAR(50),
        `count` INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );'''
    )

    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal
    )

    check_medal_task = BranchPythonOperator(
        task_id='check_medal',
        python_callable=check_medal
    )

    calc_gold_task = MySqlOperator(
        task_id='calc_gold_task',
        mysql_conn_id=connection_name,
        sql='''
        WITH gold_count AS (
            SELECT COUNT(*) AS cnt
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold'
        )
        INSERT INTO romans.medals (medal_type, count)
        SELECT 'Gold', cnt
        FROM gold_count;
        '''
    )

    calc_silver_task = MySqlOperator(
        task_id='calc_silver_task',
        mysql_conn_id=connection_name,
        sql='''
        WITH silver_count AS (
            SELECT COUNT(*) AS cnt
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver'
        )
        INSERT INTO romans.medals (medal_type, count)
        SELECT 'Silver', cnt
        FROM silver_count;
        '''
    )

    calc_bronze_task = MySqlOperator(
        task_id='calc_bronze_task',
        mysql_conn_id=connection_name,
        sql='''
        WITH bronze_count AS (
            SELECT COUNT(*) AS cnt
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze'
        )
        INSERT INTO romans.medals (medal_type, count)
        SELECT 'Bronze', cnt
        FROM bronze_count;
        '''
    )

    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=tr.ONE_SUCCESS
    )

    check_for_correctness_task = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql='''
        SELECT CASE
            WHEN MAX(created_at) IS NOT NULL
                 AND TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
            THEN 1
            ELSE 0
        END AS is_fresh
        FROM romans.medals;
        ''',
        mode='poke',
        poke_interval=5,
        timeout=10
    )

    create_schema >> create_table >> pick_medal_task >> check_medal_task
    check_medal_task >> [calc_gold_task, calc_silver_task, calc_bronze_task] >> generate_delay_task
    generate_delay_task >> check_for_correctness_task
