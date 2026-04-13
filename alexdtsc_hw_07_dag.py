from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

# --- НАЛАШТУВАННЯ ---
# Ім'я підключення до БД (як у лекції)
CONNECTION_ID = "goit_mysql_db_alexdtsc"
# Твоя персональна схема (база даних). Заміни, якщо ментор видав іншу!
MY_SCHEMA = "alexdtsc" 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, 0, 0),
}

# --- PYTHON ФУНКЦІЇ ДЛЯ ОПЕРАТОРІВ ---

# 2. Випадково обирає медаль і зберігає в XCom
def pick_medal(**kwargs):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Обрано медаль: {medal}")
    return medal

# 3. Розгалуження (Стрілочник): читає обрану медаль і направляє потік
def pick_medal_task(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')
    if medal == 'Bronze':
        return 'calc_Bronze'
    elif medal == 'Silver':
        return 'calc_Silver'
    else:
        return 'calc_Gold'

# 5. Штучна затримка на 35 секунд
def generate_delay():
    print("Засинаю на 35 секунд...")
    time.sleep(35)
    print("Прокинувся!")


# --- СТВОРЕННЯ DAG ---
with DAG(
    'alexdtsc_olympic_medals_dag',
    default_args=default_args,
    schedule_interval=None, # Запускаємо тільки вручну
    catchup=False,
    tags=['alexdtsc', 'hw_07']
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {MY_SCHEMA}.hw_dag_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2. Таска вибору медалі
    pick_medal_node = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
    )

    # 3. Таска розгалуження
    branch_node = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task,
    )

    # 4. Три таски для розрахунку та запису в БД
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {MY_SCHEMA}.hw_dag_results (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW() 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {MY_SCHEMA}.hw_dag_results (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW() 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {MY_SCHEMA}.hw_dag_results (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW() 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Gold';
        """
    )

    # 5. Затримка (виконається, якщо хоча б один з розрахунків успішний)
    delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # 6. Сенсор: перевіряє, чи найновіший запис не старший за 30 секунд
    # TIMESTAMPDIFF повертає різницю в секундах. Якщо вона <= 30, повертається 1 (True), сенсор успішний.
    # Оскільки затримка 35 секунд, тут завжди буде > 30, і сенсор впаде по тайм-ауту (як і вимагає ДЗ).
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=CONNECTION_ID,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30 
        FROM {MY_SCHEMA}.hw_dag_results;
        """,
        mode='poke',
        poke_interval=10, # Перевіряти кожні 10 секунд
        timeout=25 # Здатися (впасти) через 25 секунд очікування
    )

    # --- ВСТАНОВЛЕННЯ ЗАЛЕЖНОСТЕЙ (КРЕСЛЕННЯ КОНВЄЄРА) ---
    create_table >> pick_medal_node >> branch_node
    branch_node >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> delay_task >> check_for_correctness