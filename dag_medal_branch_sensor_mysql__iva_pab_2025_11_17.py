from __future__ import annotations

import random
import time
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.trigger_rule import TriggerRule

# =========================
# КОНФІГУРАЦІЯ DAG
# =========================
MYSQL_CONN_ID = "mysql_default"  # ID підключення у Airflow
SOURCE_TABLE = "olympic_dataset.athlete_event_results"  # таблиця з медалями
TARGET_TABLE = "olympic_dataset.medal_counts_log"       # таблиця для результатів
SLEEP_SECONDS = 35  #  для демонстрації "failed" сенсора

default_args = {
    "owner": "airflow",
    "retries": 0,
}

# =========================
# DAG ВИЗНАЧЕННЯ
# =========================
with DAG(
    dag_id="medal_branch_sensor_mysql__iva_pab_2025_11_17",  # УНІКАЛЬНИЙ ID
    description="HW: DAG з розгалуженням, підрахунком медалей, затримкою і сенсором",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,  # запуск лише вручну
    catchup=False,
    default_args=default_args,
    tags=["goit-hw07", "iva-pab"],
) as dag:

    # 1️⃣ Створення таблиці (IF NOT EXISTS)
    create_table = MySqlOperator(
        task_id="create_target_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(16) NOT NULL,
            count INT NOT NULL,
            created_at DATETIME NOT NULL
        ) ENGINE=InnoDB;
        """,
    )

    # 2️⃣ Випадковий вибір медалі + 3️⃣ Розгалуження
    def choose_medal_task(**context):
        medal = random.choice(["Bronze", "Silver", "Gold"])
        context["ti"].xcom_push(key="chosen_medal", value=medal)
        return {
            "Bronze": "count_and_insert_bronze",
            "Silver": "count_and_insert_silver",
            "Gold": "count_and_insert_gold",
        }[medal]

    choose_medal = BranchPythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal_task,
        do_xcom_push=True,
    )

    # 4️⃣ Підрахунок записів і вставка в таблицю
    def count_and_insert(medal: str):
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        # підрахунок кількості медалей
        cnt = hook.get_first(
            sql=f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE medal = %s",
            parameters=(medal,),
        )[0]
        # запис результату у лог-таблицю
        hook.run(
            sql=f"""
                INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
                VALUES (%s, %s, NOW())
            """,
            parameters=(medal, int(cnt)),
            autocommit=True,
        )

    count_and_insert_bronze = PythonOperator(
        task_id="count_and_insert_bronze",
        python_callable=lambda: count_and_insert("Bronze"),
    )

    count_and_insert_silver = PythonOperator(
        task_id="count_and_insert_silver",
        python_callable=lambda: count_and_insert("Silver"),
    )

    count_and_insert_gold = PythonOperator(
        task_id="count_and_insert_gold",
        python_callable=lambda: count_and_insert("Gold"),
    )

    # 5️⃣ Затримка перед сенсором
    def sleeper():
        time.sleep(int(SLEEP_SECONDS))

    sleep_if_success = PythonOperator(
        task_id="sleep_if_success",
        python_callable=sleeper,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # виконується, якщо хоча б одна гілка успішна
    )

    # 6️⃣ Сенсор: перевірка, що останній запис не старший за 30 секунд
    def is_latest_record_fresh(**_):
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        row = hook.get_first(
            sql=f"SELECT created_at FROM {TARGET_TABLE} ORDER BY created_at DESC LIMIT 1"
        )
        if not row or not row[0]:
            return False

        latest_created_at = row[0]
        now_utc = datetime.utcnow()
        delta = now_utc - latest_created_at

        return delta.total_seconds() <= 30  # True = свіже, False = старе (сенсор впаде)

    freshness_sensor = PythonSensor(
        task_id="freshness_sensor_le_30s",
        python_callable=is_latest_record_fresh,
        mode="poke",          # режим "перевіряти кожні кілька секунд"
        poke_interval=5,      # перевірка кожні 5 секунд
        timeout=60,           # максимум 60 секунд чекання
        soft_fail=False,      # якщо False — падає, якщо умова не виконана
    )

    # 🔗 Залежності між тасками
    create_table >> choose_medal
    choose_medal >> [
        count_and_insert_bronze,
        count_and_insert_silver,
        count_and_insert_gold,
    ]
    [
        count_and_insert_bronze,
        count_and_insert_silver,
        count_and_insert_gold,
    ] >> sleep_if_success
    sleep_if_success >> freshness_sensor
