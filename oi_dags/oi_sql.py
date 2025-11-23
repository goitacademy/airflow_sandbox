from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
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
connection_name = "oi_goit_mysql_db"

# Визначення DAG
with DAG(
        'oi_working_with_mysql_db',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["oi_sql"]  # Теги для класифікації DAG
) as dag:

    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS oleksiy;
        """
    )

    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS oleksiy.games (
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

    # Сенсор для порівняння кількості рядків у таблицях `oleksiy.games` і `olympic_dataset.games`
    check_for_data = SqlSensor(
        task_id='check_if_counts_same',
        conn_id=connection_name,
        sql="""WITH count_in_copy AS (
                select COUNT(*) nrows_copy from oleksiy.games
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

    # Завдання для оновлення даних у таблиці `oleksiy.games`
    refresh_data = MySqlOperator(
        task_id='refresh',
        mysql_conn_id=connection_name,
        sql="""
            TRUNCATE oleksiy.games;  # Очищення таблиці
            INSERT INTO oleksiy.games SELECT * FROM olympic_dataset.games;  # Вставка даних з іншої таблиці
        """,
    )

    # Завдання для примусового встановлення статусу DAG як успішного в разі невдачі
    mark_success_task = PythonOperator(
        task_id='mark_success',
        trigger_rule=tr.ONE_FAILED,  # Виконати, якщо хоча б одне попереднє завдання завершилося невдачею
        python_callable=mark_dag_success,
        provide_context=True,  # Надати контекст завдання у виклик функції
        dag=dag,
    )

    # Встановлення залежностей між завданнями
    create_schema >> create_table >> check_for_data >> refresh_data
    check_for_data >> mark_success_task