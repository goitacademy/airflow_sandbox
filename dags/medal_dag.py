import time
import random
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

# --- ЗМІННА ДЛЯ ТЕСТУВАННЯ ---
# 5 секунд = сенсор спрацює (Success)
# 35 секунд = сенсор не спрацює (Failed)
DELAY_SECONDS = 5
# -----------------------------

# ID вашого MySQL-підключення в Airflow
MYSQL_CONN_ID = "mysql_default"
# Назва таблиці з результатами (як на скріншоті)
RESULTS_TABLE = "oleksiy.hw_dag_results"
# Назва таблиці з вихідними даними
SOURCE_TABLE = "olympic_dataset.athlete_event_results"


# Функція для Завдання 2: Випадково обирає медаль
def _pick_medal():
    """Випадково обирає тип медалі та передає його в XCom."""
    medals = ["Bronze", "Silver", "Gold"]
    chosen_medal = random.choice(medals)
    print(f"Випадково обрана медаль: {chosen_medal}")
    return chosen_medal


# Функція для Завдання 3: Визначає, яку гілку запустити
def _branch_on_medal(ti):
    """
    Читає XCom з 'pick_medal' і повертає task_id
    наступного завдання для запуску.
    """
    chosen_medal = ti.xcom_pull(task_ids="pick_medal")
    print(f"Розгалуження на основі медалі: {chosen_medal}")

    # Повертаємо ID завдання, яке має запуститися
    if chosen_medal == "Bronze":
        return "calc_Bronze"
    elif chosen_medal == "Silver":
        return "calc_Silver"
    elif chosen_medal == "Gold":
        return "calc_Gold"


# Функція для Завдання 5: Затримка
def _generate_delay():
    """Просто "спить" вказану кількість секунд."""
    print(f"Запускаю затримку на {DELAY_SECONDS} секунд...")
    time.sleep(DELAY_SECONDS)
    print("Затримку завершено.")


@dag(
    dag_id="medal_branching_dag_v2",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["branching", "sql", "sensor"],
)
def medal_branching_pipeline():

    # Завдання 1: Створення таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання 2: Вибір медалі
    pick_medal = PythonOperator(task_id="pick_medal", python_callable=_pick_medal)

    # Завдання 3: Розгалуження
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=_branch_on_medal
    )

    # --- Завдання 4: Три гілки для підрахунку ---

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM {SOURCE_TABLE}
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM {SOURCE_TABLE}
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM {SOURCE_TABLE}
        WHERE medal = 'Gold';
        """,
    )

    # Завдання 5: Затримка
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=_generate_delay,
        # Це критично: завдання запуститься, якщо ХОЧА Б ОДИН
        # із батьківських (calc_*) успішно завершиться.
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Завдання 6: Сенсор
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        # Запит перевіряє, чи є в таблиці хоча б один запис,
        # створений менш ніж 30 секунд тому.
        sql=f"""
        SELECT 1
        FROM (
            SELECT MAX(created_at) as max_time
            FROM {RESULTS_TABLE}
        ) AS latest
        WHERE latest.max_time > (NOW() - INTERVAL 30 SECOND);
        """,
        poke_interval=5,  # Перевіряти кожні 5 секунд
        timeout=40,  # Максимальний час очікування = 40 сек
        mode="poke",
    )

    # --- Визначення послідовності (Dependencies) ---

    # 1 -> 2 -> 3
    create_table >> pick_medal >> pick_medal_task

    # 3 -> 4 (розгалуження)
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]

    # 4 -> 5 (з'єднання)
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay

    # 5 -> 6
    generate_delay >> check_for_correctness


# Викликаємо DAG
medal_branching_pipeline()
