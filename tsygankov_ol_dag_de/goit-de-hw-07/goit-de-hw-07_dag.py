from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

default_args = { 
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 1),
}

dag = DAG(
    dag_id='tsygankov_hw-07_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ttsygankov_hw-07_tag'],
)

# 1. Створення таблиці
create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_default',
    sql="""
        CREATE TABLE IF NOT EXISTS medals_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
    """,
    dag=dag,
)

# 2. Випадковий вибір медалі
def choose_medal(ti):
    choice = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Chosen medal: {choice}")
    ti.xcom_push(key='medal', value=choice)

pick_medal = PythonOperator(
    task_id='pick_medal',
    python_callable=choose_medal,
    dag=dag,
)

# 3. Розгалуження
def branch_task(ti):
    medal = ti.xcom_pull(task_ids='pick_medal', key='medal')
    return f'calc_{medal}'

pick_medal_task = BranchPythonOperator(
    task_id='pick_medal_task',
    python_callable=branch_task,
    dag=dag,
)

# 4. Функції для підрахунку кількості медалей
def build_insert_sql(medal_type):
    return f"""
        INSERT INTO medals_summary (medal_type, count, created_at)
        SELECT '{medal_type}', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal_type}';
    """

calc_Bronze = MySqlOperator(
    task_id='calc_Bronze',
    mysql_conn_id='mysql_default',
    sql=build_insert_sql('Bronze'),
    dag=dag,
)

calc_Silver = MySqlOperator(
    task_id='calc_Silver',
    mysql_conn_id='mysql_default',
    sql=build_insert_sql('Silver'),
    dag=dag,
)

calc_Gold = MySqlOperator(
    task_id='calc_Gold',
    mysql_conn_id='mysql_default',
    sql=build_insert_sql('Gold'),
    dag=dag,
)

# 5. Затримка
def delay_function():
    time.sleep(5)  # або 35, щоб перевірити падіння сенсора

generate_delay = PythonOperator(
    task_id='generate_delay',
    python_callable=delay_function,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# 6. Сенсор на перевірку свіжості запису
check_for_correctness = SqlSensor(
    task_id='check_for_correctness',
    mysql_conn_id='mysql_default',
    sql="""
        SELECT 1 FROM medals_summary
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY created_at DESC LIMIT 1;
    """,
    timeout=60,
    poke_interval=10,
    mode='poke',
    dag=dag,
)

# Встановлення залежностей
create_table >> pick_medal >> pick_medal_task
pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
[calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
