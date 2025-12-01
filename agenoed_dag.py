from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time


MYSQL_CONNECTION_ID = 'goit_mysql_db'
TABLE_NAME = 'olympic_dataset.agenoed_medal_count'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
}

with DAG(
    'agenoed_medal_counting_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['homework', 'agenoed']
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=MYSQL_CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2. Функція, яка обирає медаль 
    def select_random_medal(**kwargs):
        medal = random.choice(['Bronze', 'Silver', 'Gold'])
        print(f"Обрано медаль: {medal}")
        return medal

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=select_random_medal
    )

    # 3. Функція розгалуження 
    def branching_func(ti):
        selected_medal = ti.xcom_pull(task_ids='pick_medal')
        return f"calc_{selected_medal}"

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=branching_func
    )

    # 4. Завдання розрахунку
    sql_query_template = f"""
    INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
    SELECT '{{0}}', COUNT(*), NOW()
    FROM olympic_dataset.athlete_event_results
    WHERE medal = '{{0}}';
    """

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=MYSQL_CONNECTION_ID,
        sql=sql_query_template.format('Bronze')
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=MYSQL_CONNECTION_ID,
        sql=sql_query_template.format('Silver')
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=MYSQL_CONNECTION_ID,
        sql=sql_query_template.format('Gold')
    )

    # 5. Затримка
    def delay_func():
        print("Починаю затримку...")
        time.sleep(5) 
        print("Затримка завершена.")

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=delay_func,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # 6. Сенсор
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=MYSQL_CONNECTION_ID,
        sql=f"""
        SELECT COUNT(*)
        FROM {TABLE_NAME}
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        """,
        mode='poke',
        poke_interval=5,
        timeout=6,
        soft_fail=False
    )

    
    create_table >> pick_medal >> pick_medal_task
    
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay
    
    generate_delay >> check_for_correctness