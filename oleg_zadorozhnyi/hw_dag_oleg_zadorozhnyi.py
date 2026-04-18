from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime
import random
import time

# ================================================================
# Налаштування
# ================================================================
SCHEMA_NAME = "neo_data_oleg_zadorozhnyi"
CONNECTION_ID = "goit_mysql_db_oleg_zadorozhnyi"
DAG_ID = "hw_dag_oleg_zadorozhnyi"
TAG_NAME = "oleg_zadorozhnyi"

TABLE_NAME = f"{SCHEMA_NAME}.hw_dag_results"


# ---------- Python-функції ----------

def pick_medal():
    """Випадково обирає одне з трьох значень."""
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Picked medal: {medal}")
    return medal


def pick_medal_task(ti):
    """Повертає ID наступної задачі залежно від обраної медалі."""
    medal = ti.xcom_pull(task_ids='pick_medal')
    print(f"Branching to calc_{medal}")
    return f"calc_{medal}"


def generate_delay():
    """Затримка перед перевіркою сенсора.

    Для демонстрації різних станів сенсора змінюйте delay_seconds:
      - 5  секунд -> сенсор пройде (success)
      - 35 секунд -> сенсор впаде (failed, бо запис старший за 30 сек)
    """
    delay_seconds = 5
    print(f"Sleeping for {delay_seconds} seconds...")
    time.sleep(delay_seconds)
    print("Wake up!")


# ---------- Визначення DAG ----------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=None,   # запуск лише вручну
        catchup=False,
        tags=[TAG_NAME]
) as dag:

    # 1. Створення схеми і таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        CREATE DATABASE IF NOT EXISTS {SCHEMA_NAME};
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """
    )

    # 2. Випадково обирає одне з трьох значень
    pick_medal_op = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
    )

    # 3. Розгалуження: запускає одне з трьох завдань
    pick_medal_task_op = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task,
    )

    # 4. Три завдання: рахують кількість медалей відповідного типу
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # 5. Затримка (спрацює, якщо одна з трьох calc_* успішно завершилась)
    generate_delay_op = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=tr.ONE_SUCCESS,
    )

    # 6. Сенсор: найновіший запис не старший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=CONNECTION_ID,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
        FROM {TABLE_NAME};
        """,
        mode='poke',
        poke_interval=5,
        timeout=30,
    )

    # ---------- Залежності ----------
    create_table >> pick_medal_op >> pick_medal_task_op
    pick_medal_task_op >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_op >> check_for_correctness
