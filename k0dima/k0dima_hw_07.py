import random
import time
from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule


list_medals = ['Bronze', 'Silver', 'Gold']


def pick_medal_operator():
    medal = random.choice(list_medals)
    print(f"Picked medal: {medal}")
    return medal


def pick_medal_task_branch_operator(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')

    if medal == 'Bronze':
        return 'calc_Bronze'
    elif medal == 'Silver':
        return 'calc_Silver'
    else:
        return 'calc_Gold'


def generate_delay_operator(ti):
    time.sleep(35)
    print("Delay generated")


def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)


# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_k0dima"

# Визначення DAG
with DAG(
        'k0dima_homework_07_counter_medals',
        default_args=default_args,
        schedule=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["k0dima"]  # Теги для класифікації DAG
) as dag:

    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS k0dima;
        """
    )

    # Завдання для створення таблиці (якщо не існує)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS k0dima.competitions (
        `id` int AUTO_INCREMENT PRIMARY KEY,
        `medal_type` varchar(100), 
        `count` int, 
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_operator,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task_branch_operator,
    )

    calc_Bronze = SQLExecuteQueryOperator(
        task_id='calc_Bronze',
        conn_id=connection_name,
        sql="""
        INSERT INTO k0dima.competitions (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = SQLExecuteQueryOperator(
        task_id='calc_Silver',
        conn_id=connection_name,
        sql="""
        INSERT INTO k0dima.competitions (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = SQLExecuteQueryOperator(
        task_id='calc_Gold',
        conn_id=connection_name,
        sql="""
        INSERT INTO k0dima.competitions (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay_operator,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT 1
        FROM k0dima.competitions
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode='poke',
        poke_interval=5,
        timeout=6,
    )

    # Встановлення залежностей між завданнями
    create_schema >> create_table
    create_table >> pick_medal
    pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay
    generate_delay >> check_for_correctness
