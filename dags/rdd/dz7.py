from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State

import random
import time
from datetime import datetime, timedelta

# Функція для примусового встановлення статусу DAG як успішного
# def mark_dag_success(ti, **kwargs):
#     dag_run = kwargs['dag_run']
#     dag_run.set_state(State.SUCCESS)

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
    DROP TABLE IF EXISTS mds6rdd.medals;
    
    CREATE TABLE IF NOT EXISTS mds6rdd.medals (
        `id` INT NOT NULL AUTO_INCREMENT,
        `medal_type` VARCHAR(50),
        `count` INT,
        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`)
    );
    """
    )


    # Випадково обираємо одну медаль
    def choose_medal():
        return random.choice(['Bronze', 'Silver', 'Gold'])

    choose_medal_task = PythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal
    )

    # Розгалуження залежно від обраного значення
    def branch_by_medal(ti, **kwargs):
        medal = ti.xcom_pull(task_ids='choose_medal')
        if medal == 'Bronze':
            return 'count_bronze'
        elif medal == 'Silver':
            return 'count_silver'
        else:
            return 'count_gold'

    branch_task = PythonOperator(
        task_id='branch_by_medal',
        python_callable=branch_by_medal,
        provide_context=True
    )

    # 4 Завдання для підрахунку і запису в таблицю
    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO mds6rdd.medals (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal='Bronze';
        """
    )

    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO mds6rdd.medals (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal='Silver';
        """
    )

    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO mds6rdd.medals (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal='Gold';
        """
    )

    # 5 Затримка виконання наступного завдання
    def delay_task():
        time.sleep(5)  # наприклад, 5 секунд

    delay = PythonOperator(
        task_id='delay',
        python_callable=delay_task,
        trigger_rule='one_success'  # якщо одне з попередніх завдань успішно
    )

    # 6 Сенсор перевірки останнього запису
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,
        sql="""
        SELECT 1
        FROM mds6rdd.medals
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY id DESC
        LIMIT 1;
        """,
        poke_interval=5,
        timeout=35
    )

    # 🔗 Dependencies
    create_schema >> create_table >> choose_medal_task >> branch_task
    branch_task >> count_bronze >> delay >> check_recent_record
    branch_task >> count_silver >> delay >> check_recent_record
    branch_task >> count_gold >> delay >> check_recent_record
