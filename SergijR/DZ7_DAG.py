from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime, timedelta
import random
import time

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db"

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

# Функція для створення затримки
def create_delay(**context):
    print("Починаємо затримку на 35 секунд...")
    time.sleep(35)  # Затримка на 35 секунд
    print("Затримка завершена")

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Визначення DAG
with DAG(
        'medals_data_processing_RSMNYS',
        default_args=default_args,
        description='A DAG to process medal data',
        schedule_interval=None,
        catchup=False,
        tags=["medals", "homework", "RSMNYS"]
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS medals_data (
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

    # 4.1 Підрахунок записів з Bronze медалями
    calc_bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO medals_data (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Bronze';
        """
    )

    # 4.2 Підрахунок записів з Silver медалями
    calc_silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO medals_data (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Silver';
        """
    )

    # 4.3 Підрахунок записів з Gold медалями
    calc_gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO medals_data (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Gold';
        """
    )

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
        sql="""
        SELECT COUNT(*) 
        FROM medals_data 
        WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 SECOND);
        """,
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=60,  # Тайм-аут після 60 секунд
        soft_fail=True  # Дозволяє DAG продовжувати виконання навіть у разі помилки сенсора
    )

    # Визначення залежностей між завданнями
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_record_freshness