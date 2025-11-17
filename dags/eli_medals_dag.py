import random
from airflow import DAG
from datetime import datetime
import time
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State

# Function for generating a random number
def generate_medal_type(ti):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Generated number: {medal}")

    return medal

def check_medal_type(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')

    if medal == 'Bronze':
        return 'calc_Bronze'
    elif medal == 'Silver':
        return 'calc_Silver'
    else:
        return 'calc_Gold'
    
def wait_task():
    print("Sleep...")
    time.sleep(5)
    print("Ready!")

# Parameters DAG
default_args = {
    'owner': 'airflow',
}

# Name of the MySQL database connection
connection_name = "goit_mysql_db_eli"

with DAG(
    'working_with_mysql_db_eli',
    default_args=default_args,
    description='DAG for working with MySQL database',
    schedule_interval=None,  # Run only manually
    start_date=datetime(2025, 11, 17),
    catchup=False,
    tags=['eli'],
) as dag:

    # Task to create the table (if it does not exist)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS medals_eli (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `medal_type` VARCHAR(100) NOT NULL,
        `count` INT NOT NULL DEFAULT 0,
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    generate_medal_task = PythonOperator(
        task_id='pick_medal',
        python_callable=generate_medal_type,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=check_medal_type,
    )

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO medals_eli (medal_type, count)
            SELECT 'Bronze', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO medals_eli (medal_type, count)
            SELECT 'Silver', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO medals_eli (medal_type, count)
            SELECT 'Gold', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """,
    )

    generate_delay = PythonOperator(
    task_id='generate_delay',
    python_callable=wait_task,
    trigger_rule=tr.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
            SELECT 1
            FROM medals_eli
            WHERE created_at >= NOW() - INTERVAL '30 seconds'
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        mode='poke',  # Mode of checking: periodic condition check
        poke_interval=5,  # Check every 5 seconds
        timeout=6,  # Timeout after 6 seconds (1 retry)
    )

    # Setting dependencies
    create_table >> generate_medal_task >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness