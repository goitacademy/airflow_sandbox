from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta
import random
import time


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id='ola_medal_count_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_default',
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.ola_medals_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2. Випадковий вибір медалі
    def choose_medal():
        return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=lambda: print("Picking medal..."),
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=choose_medal
    )

    # 3. Три завдання під розгалуження
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='mysql_default',
        sql="""
            INSERT INTO neo_data.ola_medals_results (medal_type, count)
            SELECT 'Bronze', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='mysql_default',
        sql="""
            INSERT INTO neo_data.ola_medals_results (medal_type, count)
            SELECT 'Silver', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='mysql_default',
        sql="""
            INSERT INTO neo_data.ola_medals_results (medal_type, count)
            SELECT 'Gold', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    # 5. Створення затримки
    def delay_task():
        time.sleep(15)

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=delay_task,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 6. SQL Sensor — перевірка свіжості запису
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='mysql_default',
        sql="""
            SELECT COUNT(*)
            FROM neo_data.ola_medals_results
            WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        timeout=60,
        poke_interval=10,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Зв’язки між задачами
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
