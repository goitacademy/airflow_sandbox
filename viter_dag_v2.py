from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime
import time
import random

from airflow.sensors.sql import SqlSensor



def pick_medal_fn():
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    return medal
    # kwargs['ti'].xcom_push(key='medal_type', value=medal)

def pick_medal_task_fn(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    if medal == 'Bronze':
        return "calc_Bronze"
    elif medal == 'Silver':
        return "calc_Silver"
    else:
        return "calc_Gold"


def final_function(ti):
    chosen_medal = ti.xcom_pull(task_ids="pick_medal")
    count = ti.xcom_pull(task_ids=f"calc_{chosen_medal}", key="record_count")

    # ti.xcom_push(key='result', value=result)
    print(f"{chosen_medal} -> {count}")


def sleep_task():
    time.sleep(35)
    # time.sleep(5)


default_args = {
    "owner": 'airflow',
    "start_date": datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db_mviter_07"


# DAG Definition

with DAG(
    dag_id='mv_hw_07_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["tl_hw_07"],
    ) as dag:

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS tlkktl.games(
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_fn,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task_fn,
    )

    calc_bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO tlkktl.games (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """,
    )

    calc_silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO tlkktl.games (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO tlkktl.games (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=sleep_task,
        trigger_rule=tr.ONE_SUCCESS,

    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=connection_name,
        sql="""
            SELECT 1
            FROM tlkktl.games
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        mode="poke",
        poke_interval=5,         
        timeout=6,
    )
    end_task = PythonOperator(
        task_id="end_task",
        python_callable=final_function,
        trigger_rule=tr.ONE_SUCCESS,
    )

    # Dependencies

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    calc_bronze >> generate_delay
    calc_silver >> generate_delay
    calc_gold >> generate_delay
    generate_delay >> check_for_correctness
    check_for_correctness >> end_task
