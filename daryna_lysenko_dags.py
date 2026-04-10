from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


# Назва підключення до MySQL (має бути налаштоване в Airflow Connections)
DB_CONN = "DBneodata"

# Назва таблиці, куди будемо записувати результати
RESULT_TABLE = "medal_statistics"


# Функція, яка випадково обирає тип медалі
def get_random_medal():
    selected = random.choice(["Gold", "Silver", "Bronze"])
    print(f"Medal selected: {selected}")
    return selected


def choose_branch(**kwargs):
    task_instance = kwargs["ti"]

    # Отримуємо значення медалі з попереднього таску через XCom
    medal_value = task_instance.xcom_pull(task_ids="select_medal")

    # Відповідність: медаль → task_id
    mapping = {
        "Gold": "save_gold_count",
        "Silver": "save_silver_count",
        "Bronze": "save_bronze_count",
    }

    # Повертаємо task_id, який потрібно виконати
    return mapping[medal_value]


# Функція для створення затримки
def pause_before_check():
    print("Pause for 35 seconds before sensor check")
    time.sleep(35)  # затримка більша за 30 сек → sensor впаде


# Опис DAG
with DAG(
    dag_id="olympic_medal_stats_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airflow", "mysql", "homework"],
) as dag:

    # 1. Створення таблиці, якщо вона ще не існує
    init_table = MySqlOperator(
        task_id="init_table",
        mysql_conn_id=DB_CONN,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (
            id INT NOT NULL AUTO_INCREMENT,
            medal_type VARCHAR(10) NOT NULL,
            count INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """,
    )

    # 2. Випадковий вибір медалі
    select_medal = PythonOperator(
        task_id="select_medal",
        python_callable=get_random_medal,
    )

    # 3. Розгалуження DAG залежно від обраної медалі
    route_by_medal = BranchPythonOperator(
        task_id="route_by_medal",
        python_callable=choose_branch,
    )

    # 4. Завдання для підрахунку Gold медалей і запису в таблицю
    save_gold_count = MySqlOperator(
        task_id="save_gold_count",
        mysql_conn_id=DB_CONN,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # 4. Завдання для підрахунку Silver медалей і запису в таблицю
    save_silver_count = MySqlOperator(
        task_id="save_silver_count",
        mysql_conn_id=DB_CONN,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    # 4. Завдання для підрахунку Bronze медалей і запису в таблицю
    save_bronze_count = MySqlOperator(
        task_id="save_bronze_count",
        mysql_conn_id=DB_CONN,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    # 5. Затримка виконання перед перевіркою
    wait_task = PythonOperator(
        task_id="wait_task",
        python_callable=pause_before_check,
        # важливо: після branching тільки один таск SUCCESS, інші SKIPPED
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Сенсор перевіряє, чи останній запис не старший за 30 секунд
    recent_record_sensor = SqlSensor(
        task_id="recent_record_sensor",
        conn_id=DB_CONN,
        sql=f"""
        SELECT IF(
            MAX(created_at) > NOW() - INTERVAL 30 SECOND,
            1,
            0
        )
        FROM {RESULT_TABLE};
        """,
        poke_interval=5,  # перевірка кожні 5 секунд
        timeout=45,  # максимум 45 секунд очікування
        mode="poke",
    )

    # Побудова залежностей між тасками
    init_table >> select_medal >> route_by_medal

    # тільки одна з трьох гілок виконається
    (
        route_by_medal
        >> [save_gold_count, save_silver_count, save_bronze_count]
        >> wait_task
        >> recent_record_sensor
    )
