# Домашнє завдання до теми “Apache Airflow”



from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
import random
import time

# Функція для примусового встановлення статусу DAG як успішного
def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

def wait_some_time():
    
    time.sleep(10)  


# 2. Випадково обирає одне із трьох значень ['Bronze', 'Silver', 'Gold'].
def generate_medal(ti):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Generated medal: {medal}")

    return medal.lower()

def choose_medal(ti):
    medal = ti.xcom_pull(task_ids='generate_medal')

    if medal == 'Bronze':
        return 'process_bronze'
    if medal == 'Gold':
        return 'process_gold'
    if medal == 'Silver':
        return 'process_silver'
    

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "oi_hw_airflow"

# Визначення DAG
with DAG(
        'oi_hw_airflow',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["oi_hw"]  # Теги для класифікації DAG
) as dag:


#1. Створює таблицю.
    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS oi_hw;
        """
    )

    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS oi_hw.medals (
        `id` INT AUTO_INCREMENT PRIMARY KEY
        `medal_type` TEXT,
        `count` INT,
        `created_at` DATE DEFAULT NULL
        );
        """
    )

    generate_medal_task = PythonOperator(
        task_id='generate_medal',
        python_callable=generate_medal,
    )

# 3. Залежно від обраного значення запускає одне із трьох завдань (розгалуження).
    choose_medal_task = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal,
    )


# 4. Опис трьох завдань:
    process_bronze = MySqlOperator(
        task_id="process_bronze",
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO oi_hw.medals (medal_type, count, created_at)
            VALUES ("Bronze", (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE olympic_dataset.athlete_event_results.medal = 'Bronze'), Now())
        """
    )

    process_silver = MySqlOperator(
        task_id="process_silver",
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO oi_hw.medals (medal_type, count, created_at)
            VALUES ("Silver", (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE olympic_dataset.athlete_event_results.medal = 'Silver'), Now())
        """
    )

    process_gold = MySqlOperator(
        task_id="process_gold",
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO oi_hw.medals (medal_type, count, created_at)
            VALUES ("Gold", (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE olympic_dataset.athlete_event_results.medal = 'Gold'), Now())
        """
    )

# 5. Запускає затримку виконання наступного завдання.
# 👉🏻 Використайте PythonOperaor із функцією time.sleep(n), якщо одне з трьох попередніх завдань виконано успішно.
    delay_task = PythonOperator(
        task_id="delay_after_insert",
        python_callable=wait_some_time,
        trigger_rule=tr.ONE_SUCCESS, 
    )

#6. Перевіряє за допомогою сенсора, чи найновіший запис у таблиці, створеній на етапі 1, не старший за 30 секунд 
# (порівнюючи з поточним часом). Ідея в тому, щоб упевнитися, чи справді відбувся запис у таблицю.
    check_for_data = SqlSensor(
        task_id='check_if_updated',
        conn_id=connection_name,
        sql="""
            SELECT * FROM oi_hw.medals
            WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        """,
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (1 повторна перевірка)
    )

    # Завдання для оновлення даних у таблиці `oleksiy.games`
    refresh_data = MySqlOperator(
        task_id='refresh',
        mysql_conn_id=connection_name,
        sql="""
            TRUNCATE oksana.games;  # Очищення таблиці
            INSERT INTO oksana.games SELECT * FROM olympic_dataset.games;  # Вставка даних з іншої таблиці
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
    create_schema >> create_table >> check_for_data >> generate_medal_task >> choose_medal_task
    choose_medal_task >> [process_gold, process_bronze, process_silver]
    process_gold >> delay_task >> check_for_data
    process_bronze >> delay_task >> check_for_data
    process_silver >> delay_task >> check_for_data
   