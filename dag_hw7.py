from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
import random
import time

default_args = {
    'owner': 'olesia',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': 60  # секунд
}

with DAG(
    dag_id='hw7_olesia_medal_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='DBneodata',  # <- моє з'єднання
        sql="""
            CREATE TABLE IF NOT EXISTS medal_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # Випадковий вибір медалі
    def choose():
        return random.choice(['bronze_task', 'silver_task', 'gold_task'])

    choose_medal = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose
    )

    # Запис для Bronze
    bronze_task = MySqlOperator(
        task_id='bronze_task',
        mysql_conn_id='DBneodata',
        sql="""
            INSERT INTO medal_summary (medal_type, count)
            SELECT 'Bronze', COUNT(*) FROM athlete_event_results WHERE medal = 'Bronze';
        """
    )

    # Запис для Silver
    silver_task = MySqlOperator(
        task_id='silver_task',
        mysql_conn_id='DBneodata',
        sql="""
            INSERT INTO medal_summary (medal_type, count)
            SELECT 'Silver', COUNT(*) FROM athlete_event_results WHERE medal = 'Silver';
        """
    )

    # Запис для Gold
    gold_task = MySqlOperator(
        task_id='gold_task',
        mysql_conn_id='DBneodata',
        sql="""
            INSERT INTO medal_summary (medal_type, count)
            SELECT 'Gold', COUNT(*) FROM athlete_event_results WHERE medal = 'Gold';
        """
    )

    # Затримка виконання
    def delay():
        time.sleep(35)  # спробуй пізніше змінити на 25, щоб сенсор не "падав"

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay
    )

    # Сенсор: перевірка часу останнього запису
    sensor = SqlSensor(
        task_id='check_recent',
        conn_id='DBneodata',
        sql="""
            SELECT CASE
                WHEN TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30 THEN 1
                ELSE 0
            END
            FROM medal_summary;
        """,
        mode='poke',
        timeout=60,
        poke_interval=10
    )

    # Зв'язки між завданнями
    create_table >> choose_medal
    choose_medal >> [bronze_task, silver_task, gold_task] >> delay_task >> sensor
