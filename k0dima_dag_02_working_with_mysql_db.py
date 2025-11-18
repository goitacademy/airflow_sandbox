from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State

# Функція для примусового встановлення статусу DAG як успішного


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
    'k0dima_dag_02_working_with_mysql_db_18_11',
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
        CREATE TABLE IF NOT EXISTS k0dima.games (
        `edition` text,
        `edition_id` int DEFAULT NULL,
        `edition_url` text,
        `year` int DEFAULT NULL,
        `city` text,
        `country_flag_url` text,
        `country_noc` text,
        `start_date` text,
        `end_date` text,
        `competition_date` text,
        `isHeld` text
        );
        """
    )

    # Сенсор для порівняння кількості рядків у таблицях `k0dima.games` і `olympic_dataset.games`
    check_for_data = SqlSensor(
        task_id='check_if_counts_same',
        conn_id=connection_name,
        sql="""WITH count_in_copy AS (
                select COUNT(*) nrows_copy from k0dima.games
                ),
                count_in_original AS (
                select COUNT(*) nrows_original from olympic_dataset.games
                )
               SELECT nrows_copy <> nrows_original FROM count_in_copy
               CROSS JOIN count_in_original
               ;""",
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (1 повторна перевірка)
    )

    # Завдання для оновлення даних у таблиці `k0dima.games`
    refresh_data = SQLExecuteQueryOperator(
        task_id='refresh',
        conn_id=connection_name,
        sql="""
            TRUNCATE k0dima.games;  # Очищення таблиці
            INSERT INTO k0dima.games SELECT * FROM olympic_dataset.games;  # Вставка даних з іншої таблиці
        """,
    )

    # Завдання для примусового встановлення статусу DAG як успішного в разі невдачі
    mark_success_task = PythonOperator(
        task_id='mark_success',
        # Виконати, якщо хоча б одне попереднє завдання завершилося невдачею
        trigger_rule=tr.ONE_FAILED,
        python_callable=mark_dag_success,
        dag=dag,
    )

    # Встановлення залежностей між завданнями
    create_schema >> create_table >> check_for_data >> refresh_data
    check_for_data >> mark_success_task
