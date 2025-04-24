from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
import random
import time

def choose_medal():
    return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

def generate_delay():
    time.sleep(35)

with DAG('medal_alerts_dag',
         start_date=pendulum.today('UTC').add(days=-1),
         schedule='@daily',
         catchup=False) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='mysql_default',
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        hook_params={"schema": "olympic_dataset"}
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=choose_medal
    )

    calc_Bronze = SQLExecuteQueryOperator(
        task_id='calc_Bronze',
        conn_id='mysql_default',
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal='Bronze';
        """,
        hook_params={"schema": "olympic_dataset"}
    )

    calc_Silver = SQLExecuteQueryOperator(
        task_id='calc_Silver',
        conn_id='mysql_default',
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal='Silver';
        """,
        hook_params={"schema": "olympic_dataset"}
    )

    calc_Gold = SQLExecuteQueryOperator(
        task_id='calc_Gold',
        conn_id='mysql_default',
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal='Gold';
        """,
        hook_params={"schema": "olympic_dataset"}
    )

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay
    )

    check_for_correctness = SQLExecuteQueryOperator(
        task_id='check_for_correctness',
        conn_id='mysql_default',
        sql="""
        SELECT CASE
          WHEN TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) < 30
          THEN TRUE ELSE FALSE
        END AS recent_entry
        FROM olympic_dataset.medal_counts;
        """,
        hook_params={"schema": "olympic_dataset"}
    )

    create_table >> pick_medal_task >> [calc_Silver, calc_Gold, calc_Bronze] >> generate_delay >> check_for_correctness
