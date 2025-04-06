from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
from datetime import datetime, timedelta
import random
import time

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_sergijr"

# Ваша схема для створення таблиці
your_schema = "sergijr"

# Схема з даними про медалі
data_schema = "olympic_dataset"

# Функція для генерації випадкового типу медалі
def choose_medal_type():
    medal_types = ['Bronze', 'Silver', 'Gold']
    medal = random.choice(medal_types)
    print(f"Обрано тип медалі: {medal}")
    return medal


# Функція для визначення наступного завдання на основі типу медалі
def pick_medal_branch(**context):
    medal_type = context['task_instance'].xcom_pull(task_ids='pick_medal')
    if medal_type == 'Bronze':
        return 'calc_Bronze'
    elif medal_type == 'Silver':
        return 'calc_Silver'
    else:
        return 'calc_Gold'


# Функція для створення оператора підрахунку медалей
def create_medal_counter(medal_type):
    return MySqlOperator(
        task_id=f'calc_{medal_type}',
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {your_schema}.hw_dag_results (medal_type, count)
        SELECT '{medal_type}', COUNT(*) 
        FROM {data_schema}.athlete_event_results 
        WHERE medal = '{medal_type}';
        """
    )


# Функція для створення затримки
def create_delay(**context):
    print("Починаємо затримку на 35 секунд...")
    time.sleep(35)  # Затримка на 35 секунд
    print("Затримка завершена")


# Функція для фіналізації DAG
def finalize_dag(**kwargs):
    print("Фіналізація DAG виконання")
    return "DAG completed successfully"


# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Визначення DAG
with DAG(
        f'{your_schema}_medals_data_processing',
        default_args=default_args,
        description='A DAG to process medal data',
        schedule_interval=None,
        catchup=False,
        tags=[your_schema, "medals", "homework"]
) as dag:
    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql=f"""
        CREATE DATABASE IF NOT EXISTS {your_schema};

        CREATE TABLE IF NOT EXISTS {your_schema}.hw_dag_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2. Випадковий вибір типу медалі
    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=choose_medal_type,
    )

    # 3. Розгалуження залежно від типу медалі
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_branch,
        provide_context=True,
    )

    # 4. Створення операторів підрахунку для кожного типу медалі
    calc_bronze = create_medal_counter('Bronze')
    calc_silver = create_medal_counter('Silver')
    calc_gold = create_medal_counter('Gold')

    # 5. Затримка виконання
    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=create_delay,
        trigger_rule=tr.ONE_SUCCESS,  # Виконується, якщо одне з попередніх завдань успішне
    )

    # 6. Перевірка свіжості запису
    check_record_freshness = SqlSensor(
        task_id='check_record_freshness',
        conn_id=connection_name,
        sql=f"""
        SELECT COUNT(*) 
        FROM {your_schema}.hw_dag_results 
        WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 SECOND);
        """,
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=60,  # Тайм-аут після 60 секунд
    )

    # 7. Фінальний оператор для завершення DAG
    end_task = PythonOperator(
        task_id='end_task',
        python_callable=finalize_dag,
        trigger_rule=tr.ALL_DONE,  # Виконується після всіх завдань, незалежно від їх статусу
    )

    # Визначення залежностей між завданнями
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_record_freshness >> end_task