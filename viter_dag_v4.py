from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.sql import SqlSensor

from datetime import datetime
import time
import random


def pick_medal():
    return random.choice(['Bronze', 'Silver', 'Gold'])


def decide_next_task(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def log_result(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    count = ti.xcom_pull(task_ids=f"calc_{medal}", key="record_count")
    print(f"{medal} -> {count}")


def delay_task():
    time.sleep(35)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4),
}

conn_id = "goit_mysql_db_mviter_07"

with DAG(
    dag_id="mv_hw_07_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["mv_hw_07"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=conn_id,
        sql="""
            CREATE TABLE IF NOT EXISTS tlkktl.games (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(50),
                count INT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_medal_task",
        python_callable=decide_next_task,
    )

    calc_bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=conn_id,
        sql="""
            INSERT INTO tlkktl.games (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """,
    )

    calc_silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=conn_id,
        sql="""
            INSERT INTO tlkktl.games (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """,
    )

    calc_gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=conn_id,
        sql="""
            INSERT INTO tlkktl.games (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """,
    )

    delay = PythonOperator(
        task_id="delay",
        python_callable=delay_task,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    verify_insertion = SqlSensor(
        task_id="verify_insertion",
        conn_id=conn_id,
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

    final_task = PythonOperator(
        task_id="final_task",
        python_callable=log_result,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # DAG dependencies
    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> delay >> verify_insertion >> final_task
