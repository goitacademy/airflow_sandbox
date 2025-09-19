from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime
import random
import time

# Аргументи за замовчуванням
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1, 0, 0),
}

# Назва DAG
with DAG(
    dag_id='hw_medals_zoryana',
    default_args=default_args,
    schedule_interval=None,  # запуск вручну
    catchup=False,
    tags=["zoryana", "hw07"]
) as dag:

    # Завдання 1: створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db',
        sql="""
        CREATE TABLE IF NOT EXISTS zoryana_hw07_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(20),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Завдання 2: випадковий вибір медалі
    def pick_medal_func(ti):
        medal = random.choice(['Bronze', 'Silver', 'Gold'])
        print(f"Picked medal: {medal}")
        ti.xcom_push(key='medal', value=medal)

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_func,
    )

    # Завдання 3: розгалуження залежно від вибору медалі
    def branch_medal_func(ti):
        medal = ti.xcom_pull(task_ids='pick_medal', key='medal')
        if medal == 'Bronze':
            return 'calc_bronze'
        elif medal == 'Silver':
            return 'calc_silver'
        else:
            return 'calc_gold'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_medal_func,
    )

    # Завдання 4.1: підрахунок Bronze
    calc_bronze = MySqlOperator(
        task_id='calc_bronze',
        mysql_conn_id='goit_mysql_db',
        sql="""
        INSERT INTO zoryana_hw07_results (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    # Завдання 4.2: підрахунок Silver
    calc_silver = MySqlOperator(
        task_id='calc_silver',
        mysql_conn_id='goit_mysql_db',
        sql="""
        INSERT INTO zoryana_hw07_results (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    # Завдання 4.3: підрахунок Gold
    calc_gold = MySqlOperator(
        task_id='calc_gold',
        mysql_conn_id='goit_mysql_db',
        sql="""
        INSERT INTO zoryana_hw07_results (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # Завдання 5: затримка
    def delay_func():
        print("Sleeping for 10 seconds...")
        time.sleep(10)

    delay_task = PythonOperator(
    task_id='delay_task',
    python_callable=delay_func,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Завдання 6: сенсор для перевірки, що запис не старший за 30 секунд
    check_recent_insert = SqlSensor(
        task_id='check_recent_insert',
        conn_id='goit_mysql_db',
        sql="""
            SELECT CASE
                WHEN TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
                THEN 1
                ELSE 0
            END AS is_recent
            FROM zoryana_hw07_results;
        """,
        mode='poke',
        poke_interval=5,
        timeout=40,  # трохи більше ніж 30, щоб дати шанс сенсору спрацювати
    )

    # Залежності
    create_table >> pick_medal >> branch_task
    branch_task >> [calc_bronze, calc_silver, calc_gold]
    calc_bronze >> delay_task
    calc_silver >> delay_task
    calc_gold >> delay_task
    delay_task >> check_recent_insert



