from airflow import DAG
from datetime import datetime, timedelta
import time
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import random

connection_name = "goit_mysql_db"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, 0, 0),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def get_medal(ti):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Generated number: {medal}")
    return medal

def get_medal_task(ti):
    medal = ti.xcom_pull(task_ids='get_medal')
    if medal == 'Bronze':
        return 'count_Bronze'
    elif medal == 'Silver':
        return 'count_Silver'
    elif medal == 'Gold':
        return 'count_Gold'

def generate_delay(ti):
    delay = random.randint(5, 35)
    time.sleep(delay)

with DAG(
        'Alex_Sh_hw_07',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['AlexSh']
) as dag:

    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS AlexSh;
        """
    )

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS AlexSh.medals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(255) NOT NULL,
        count INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """
    )

    get_medal = PythonOperator(
        task_id='get_medal',
        python_callable=get_medal,
    )

    get_medal_task = BranchPythonOperator(
        task_id='get_medal_task',
        python_callable=get_medal_task,
    )

    count_Bronze = MySqlOperator(
        task_id='count_Bronze',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO AlexSh.medals (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*) , NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
            """,
    )

    count_Silver = MySqlOperator(
        task_id='count_Silver',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO AlexSh.medals (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*) , NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
            """,
    )

    count_Gold = MySqlOperator(
        task_id='count_Gold',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO AlexSh.medals (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*) , NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
            """,
    )

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule='one_success',
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=connection_name,
        sql="""
        SELECT 1
        FROM AlexSh.medals
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode="poke",
        poke_interval=5,
        timeout=6,
    )


    create_schema >> create_table >> get_medal >> get_medal_task >> [count_Bronze, count_Silver, count_Gold]
    count_Bronze >> generate_delay
    count_Silver >> generate_delay
    count_Gold >> generate_delay
    generate_delay >> check_for_correctness
