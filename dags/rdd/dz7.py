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

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_mds6rdd"

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}




# Визначення DAG
with DAG(
        'mds6rdd_dz7',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["mds6rdd"]  # Теги для класифікації DAG
) as dag:

    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS mds6rdd;
        """
    )

    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS mds6rdd.games (
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

    check_db = MySqlOperator(
    task_id='check_db',
    mysql_conn_id=connection_name,
    sql="SHOW DATABASES LIKE 'mds6rdd';"
)

create_schema >> create_table >> check_db

