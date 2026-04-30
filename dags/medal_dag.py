from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor


# Функція, яка випадково обирає медаль
def pick_random_medal(**kwargs):
    medal = random.choice(["Bronze", "Silver", "Gold"])  # випадковий вибір
    print(f"Selected medal: {medal}")  # виводимо в лог Airflow
    return medal  # повертаємо значення (збережеться в XCom)


# Функція для розгалуження (яку гілку запускати)
def choose_branch(**kwargs):
    ti = kwargs["ti"]  # доступ до контексту Airflow
    medal = ti.xcom_pull(task_ids="pick_medal")  # беремо результат попередньої задачі

    # повертаємо task_id тієї задачі, яку потрібно виконати
    if medal == "Bronze":
        return "calc_bronze"
    elif medal == "Silver":
        return "calc_silver"
    else:
        return "calc_gold"


# Базові параметри DAG
default_args = {
    "owner": "Svitlana",
    "start_date": datetime(2024, 1, 1),
}


# Опис DAG
with DAG(
    dag_id="medal_count_dag",  # назва DAG в Airflow
    default_args=default_args,
    schedule_interval=None,  # запуск тільки вручну
    catchup=False,  # не виконувати старі запуски
    tags=["homework", "airflow", "medals"],
) as dag:

    # 1. Створення таблиці (якщо ще не існує)
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="mysql_default",
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
    )

    # 2. Випадковий вибір медалі
    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_random_medal,
    )

    # 3. Розгалуження — визначає, яку задачу запускати далі
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    # 4.1 Підрахунок Bronze і запис у таблицю
    calc_bronze = MySqlOperator(
        task_id="calc_bronze",
        mysql_conn_id="mysql_default",
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    # 4.2 Підрахунок Silver і запис у таблицю
    calc_silver = MySqlOperator(
        task_id="calc_silver",
        mysql_conn_id="mysql_default",
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    # 4.3 Підрахунок Gold і запис у таблицю
    calc_gold = MySqlOperator(
        task_id="calc_gold",
        mysql_conn_id="mysql_default",
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # 5. Затримка 35 секунд (щоб сенсор міг "впасти")
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=lambda: time.sleep(35),  # пауза 35 секунд
        trigger_rule="one_success",  # запускається, якщо хоча б одна задача успішна
    )

    # 6. Сенсор — перевіряє, чи є "свіжий" запис (не старший 30 сек)
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id="mysql_default",
        sql="""
        SELECT COUNT(*)
        FROM olympic_dataset.medal_counts
        WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        mode="poke",  # перевіряє кожні N секунд
        timeout=60,  # максимум 60 сек очікування
        poke_interval=10,  # перевірка кожні 10 сек
    )

    # Побудова залежностей (логіка виконання)

    create_table >> pick_medal >> pick_medal_task

    # після розгалуження виконується тільки одна з трьох задач
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]

    # після будь-якої з них — затримка і сенсор
    [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_for_correctness