import time
import random
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

# -----------------------------------------
# Налаштування
# -----------------------------------------
MYSQL_CONN_ID = "mysql_default"  # ID підключення в Airflow
RESULTS_TABLE = "olympic_dataset.hw_dag_results"  # Куди пишемо результати
SOURCE_TABLE = "olympic_dataset.athlete_event_results"

# 5 сек → сенсор пройде; 35 сек → сенсор впаде (перевірка умови з 30 сек)
DELAY_SECONDS = 5
# -----------------------------------------


# 2. Випадковий вибір медалі
def _pick_medal():
    medals = ["Bronze", "Silver", "Gold"]
    chosen = random.choice(medals)
    print(f"Випадково обрана медаль: {chosen}")
    return chosen


# 3. Розгалуження залежно від вибраної медалі
def _branch_on_medal(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    if medal == "Bronze":
        return "calc_Bronze"
    if medal == "Silver":
        return "calc_Silver"
    return "calc_Gold"


# 5. Затримка виконання
def _generate_delay():
    print(f"Затримую виконання на {DELAY_SECONDS} сек...")
    time.sleep(DELAY_SECONDS)
    print("Затримку завершено.")


@dag(
    dag_id="medal_branching_dag_v3",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["branching", "sql", "sensor"],
)
def medal_branching_pipeline():
    # 1. Створення таблиці (IF NOT EXISTS)
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

    # 2. Вибір медалі
    pick_medal = PythonOperator(task_id="pick_medal", python_callable=_pick_medal)

    # 3. Розгалуження
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=_branch_on_medal
    )

    # 4. Три гілки з підрахунком і записом
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM {SOURCE_TABLE} WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM {SOURCE_TABLE} WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM {SOURCE_TABLE} WHERE medal = 'Gold';
        """,
    )

    # 5. Затримка (виконується, якщо УСПІШНА хоча б одна з гілок)
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=_generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # ключова умова
    )

    # 6. Сенсор: перевірка, що останній запис не старіший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        SELECT 1
        FROM (
            SELECT MAX(created_at) AS max_time
            FROM {RESULTS_TABLE}
        ) AS latest
        WHERE latest.max_time > (NOW() - INTERVAL 30 SECOND);
        """,
        poke_interval=5,
        timeout=40,  # 35 cек затримки → сенсор має впасти
        mode="poke",
    )

    # Залежності
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay
    generate_delay >> check_for_correctness


dag = medal_branching_pipeline()
