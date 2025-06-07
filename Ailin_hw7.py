from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time

default_args = {
    'start_date': datetime(2025, 6, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='medal_pipeline_hw7',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    create_table = MySqlOperator(
        task_id='create_table',
        sql="""
        CREATE TABLE IF NOT EXISTS hw_dag_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
        mysql_conn_id='mysql_ilin'
    )

    def choose_medal():
        return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=lambda: print("Medal picked")
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=choose_medal
    )

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        sql="""
        INSERT INTO hw_dag_results (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
        mysql_conn_id='mysql_ilin'
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        sql="""
        INSERT INTO hw_dag_results (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
        mysql_conn_id='mysql_ilin'
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        sql="""
        INSERT INTO hw_dag_results (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
        mysql_conn_id='mysql_ilin'
    )

    def sleep_func():
        time.sleep(35) 
    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=sleep_func
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='mysql_ilin',
        sql="""
        SELECT 1 FROM hw_dag_results 
        WHERE created_at > NOW() - INTERVAL 30 SECOND 
        ORDER BY created_at DESC LIMIT 1;
        """,
        mode='reschedule',
        timeout=60,
        poke_interval=10
    )

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
