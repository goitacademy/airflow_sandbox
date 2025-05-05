from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import random
import time
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='hw7_olesia_medal_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['hw7', 'medals'],
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='DBneodata',
        sql="""
            CREATE TABLE IF NOT EXISTS olesia_hw_dag_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # 2. Вибір типу медалі
    def choose_medal():
        medals = ['gold', 'silver', 'bronze']
        return f'{random.choice(medals)}_task'

    choose_medal = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal,
    )

    # 3. Оператори медалей
    insert_gold = MySqlOperator(
        task_id='gold_task',
        mysql_conn_id='DBneodata',
        sql="""
            INSERT INTO olesia_hw_dag_results (medal_type, count)
            VALUES ('gold', FLOOR(1 + RAND() * 10));
        """,
    )

    insert_silver = MySqlOperator(
        task_id='silver_task',
        mysql_conn_id='DBneodata',
        sql="""
            INSERT INTO olesia_hw_dag_results (medal_type, count)
            VALUES ('silver', FLOOR(1 + RAND() * 10));
        """,
    )

    insert_bronze = MySqlOperator(
        task_id='bronze_task',
        mysql_conn_id='DBneodata',
        sql="""
            INSERT INTO olesia_hw_dag_results (medal_type, count)
            VALUES ('bronze', FLOOR(1 + RAND() * 10));
        """,
    )

    # 4. Очікування (штучна затримка)
    def delay():
        time.sleep(10)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 5. Перевірка останнього запису
    check_recent = SqlSensor(
        task_id='check_recent',
        conn_id='DBneodata',
        sql="""
            SELECT COUNT(*) FROM olesia_hw_dag_results
            WHERE created_at > NOW() - INTERVAL 1 MINUTE;
        """,
        mode='poke',
        timeout=30,
        poke_interval=5,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    create_table >> choose_medal >> [insert_gold, insert_silver, insert_bronze]
    [insert_gold, insert_silver, insert_bronze] >> delay_task >> check_recent
