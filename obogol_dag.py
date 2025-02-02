# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import random
import time
from datetime import datetime

from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr

medals = ['Bronze', 'Silver', 'Gold']


def pick_medal():
    return random.choice(medals)


def pick_medal_func(ti):
    medal_choice = ti.xcom_pull(task_ids='pick_medal')
    return "calc_" + medal_choice


def add_delay():
    time.sleep(1)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db_obogol"

# Визначення DAG
with DAG(
        'olympics_hw_obogol',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["obogol"]
) as dag:
    create_table_task = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.olena_hw_dag (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(255),
            count INT,
            created_at DATETIME
        );
        """
    )

    pick_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
    )

    branch_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_func,
    )

    calc_bronze_task = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO neo_data.olena_hw_dag (medal_type, count, created_at)
                SELECT medal, COUNT(*), NOW()
                FROM olympic_dataset.athlete_event_results
                WHERE medal = "Bronze"
                GROUP BY medal;
        """,
    )

    calc_silver_task = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
                INSERT INTO neo_data.olena_hw_dag (medal_type, count, created_at)
                    SELECT medal, COUNT(*), NOW()
                    FROM olympic_dataset.athlete_event_results
                    WHERE medal = "Silver"
                    GROUP BY medal;
            """,
    )

    calc_gold_task = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
                INSERT INTO neo_data.olena_hw_dag (medal_type, count, created_at)
                SELECT medal, COUNT(*), NOW()
                FROM olympic_dataset.athlete_event_results
                WHERE medal = "Gold"
                GROUP BY medal;
            """,
    )

    delay = PythonOperator(
        task_id='generate_delay',
        python_callable=add_delay,
        trigger_rule=tr.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""SELECT NOW()-created_at < 30 FROM neo_data.olena_hw_dag hdr 
                ORDER BY id DESC 
                LIMIT 1;""",
        mode='poke',
        poke_interval=15,
        timeout=31,
    )

    create_table_task >> pick_medal_task >> branch_medal_task
    branch_medal_task >> calc_bronze_task
    branch_medal_task >> calc_silver_task
    branch_medal_task >> calc_gold_task
    calc_bronze_task >> delay
    calc_silver_task >> delay
    calc_gold_task >> delay
    delay >> check_for_correctness