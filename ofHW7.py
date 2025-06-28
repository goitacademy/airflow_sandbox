from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='of_medal_count_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # без розкладу, запускається вручну
    catchup=False
) as dag:

    # Створення таблиці neo_data.zubko_medals_results
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='mysql_default',
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.zubko_medals_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        hook_params={'schema': 'neo_data'}
    )

    # Рандомний вибір медалі
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

    # Підрахунок та запис у таблицю для Bronze
    calc_Bronze = SQLExecuteQueryOperator(
        task_id='calc_Bronze',
        conn_id='mysql_default',
        sql="""
            INSERT INTO neo_data.zubko_medals_results (medal_type, count)
            SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze';
        """,
        hook_params={'schema': 'neo_data'}
    )

    # Підрахунок та запис у таблицю для Silver
    calc_Silver = SQLExecuteQueryOperator(
        task_id='calc_Silver',
        conn_id='mysql_default',
        sql="""
            INSERT INTO neo_data.zubko_medals_results (medal_type, count)
            SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver';
        """,
        hook_params={'schema': 'neo_data'}
    )

    # Підрахунок та запис у таблицю для Gold
    calc_Gold = SQLExecuteQueryOperator(
        task_id='calc_Gold',
        conn_id='mysql_default',
        sql="""
            INSERT INTO neo_data.zubko_medals_results (medal_type, count)
            SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold';
        """,
        hook_params={'schema': 'neo_data'}
    )

    # Затримка 35 секунд
    def delay_task():
        time.sleep(35)

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=delay_task
    )

    # Сенсор перевіряє, чи є новий запис не старший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='mysql_default',
        sql="""
            SELECT COUNT(*) FROM neo_data.zubko_medals_results
            WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        poke_interval=10,
        timeout=60,
        mode='poke',
        hook_params={'schema': 'neo_data'}
    )

    # Залежності
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
