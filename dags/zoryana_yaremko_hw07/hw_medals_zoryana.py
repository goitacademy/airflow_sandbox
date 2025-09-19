from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time


# Аргументи за замовчуванням
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
}

with DAG(
    dag_id='hw_medals_zoryana',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["zoryana", "hw07"],
) as dag:

    # 1) створюємо таблицю
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db',
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.zoryana_hw07_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(20),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2) випадковий вибір медалі
    def pick_medal_func(ti):
        medal = random.choice(['Bronze', 'Silver', 'Gold'])
        print(f"Picked medal: {medal}")
        ti.xcom_push(key='medal', value=medal)

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_func,
    )

    # 3) розгалуження
    def branch_func(ti):
        medal = ti.xcom_pull(task_ids='pick_medal', key='medal')
        if medal == 'Bronze':
            return 'calc_bronze'
        elif medal == 'Silver':
            return 'calc_silver'
        else:
            return 'calc_gold'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
    )

    # 4) завдання підрахунку
    calc_bronze = MySqlOperator(
        task_id='calc_bronze',
        mysql_conn_id='goit_mysql_db',
        sql="""
        INSERT INTO olympic_dataset.zoryana_hw07_results (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_silver = MySqlOperator(
        task_id='calc_silver',
        mysql_conn_id='goit_mysql_db',
        sql="""
        INSERT INTO olympic_dataset.zoryana_hw07_results (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_gold = MySqlOperator(
        task_id='calc_gold',
        mysql_conn_id='goit_mysql_db',
        sql="""
        INSERT INTO olympic_dataset.zoryana_hw07_results (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # 5) затримка
    def delay_func():
        print("Sleeping for 10 seconds...")
        time.sleep(10)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay_func,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6) сенсор
    check_recent_insert = SqlSensor(
        task_id='check_recent_insert',
        conn_id='goit_mysql_db',
        sql="""
            SELECT 1
            FROM olympic_dataset.zoryana_hw07_results
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        mode='poke',
        poke_interval=10,
        timeout=60,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # залежності
    create_table >> pick_medal >> branch_task
    branch_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> delay_task >> check_recent_insert





