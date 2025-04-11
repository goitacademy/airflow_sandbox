from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import random
import time

# Функція для генерації випадкового значення медалі
def pick_medal_func(ti):
    medal = random.choice(['gold', 'silver', 'bronze'])
    print(f"Generated medal: {medal}")

    return medal

# Функція для вибору наступного завдання на основі вибраної медалі
def pick_medal_task_func(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')

    if medal == 'gold':
        return 'calc_Gold'
    elif medal == 'silver':
        return 'calc_Silver'
    else:
        return 'calc_Bronze'
    
# Функція для підрахунку медалей
# Використовує MySqlHook для підключення до бази даних
# Підраховує кількість медалей певного типу та зберігає результат у таблиці
def calc_medal_count(medal_type):
    mysql_hook = MySqlHook(mysql_conn_id=connection_name)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    query = f"SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = '{medal_type}'"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    
    query = f"INSERT INTO olympic_dataset.{my_tag}_medals (medal_type, count, created_at) VALUES ('{medal_type}', {count}, NOW())"
    cursor.execute(query)
    conn.commit()

def calc_gold_func(ti):
    calc_medal_count('Gold')

def calc_silver_func(ti):
    calc_medal_count('Silver')

def calc_bronze_func(ti):
    calc_medal_count('Bronze')

# Функція для генерації затримки
def generate_dalay_func(ti):
    delay = random.randint(25, 35)
    time.sleep(delay)
    print(f"{delay}-secs delay generated")
    # return delay


# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 10, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_v_vasyliev"

my_tag = 'v_vasyliev'


# Визначення DAG
with DAG(
        'VVV_workflow_hw7',
        default_args=default_args,
        schedule_interval='*/10 * * * *',  # every 10 minutes
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=[f"{my_tag}"]  # Теги для класифікації DAG
) as my_dag:


    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        dag=my_dag,
        sql=f"""
        CREATE TABLE IF NOT EXISTS olympic_dataset.{my_tag}_medals (
        id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
        medal_type text,
        count int,
        created_at DATETIME
        );
        """
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_func,
        dag=my_dag,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task_func,
        dag=my_dag,
    )

    calc_Gold = PythonOperator(
        task_id='calc_Gold',
        python_callable=calc_gold_func,
        dag=my_dag,
    )

    calc_Silver = PythonOperator(
        task_id='calc_Silver',
        python_callable=calc_silver_func,
        dag=my_dag,
    )

    calc_Bronze = PythonOperator(
        task_id='calc_Bronze',
        python_callable=calc_bronze_func,
        dag=my_dag,
    )

    generate_dalay = PythonOperator(
        task_id='generate_dalay',
        python_callable=generate_dalay_func,
        trigger_rule='all_done',
        dag=my_dag,
    )

    # Сенсор для отримання факту внесення запису в таблицю
    # Використовується для перевірки, чи запис було внесено в таблицю
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql=f"""SELECT COUNT(*) FROM olympic_dataset.{my_tag}_medals
                WHERE id = (SELECT MAX(id) FROM olympic_dataset.{my_tag}_medals) 
                AND TIMESTAMPDIFF(SECOND, created_at, CURRENT_TIMESTAMP()) <= 30
               ;""",
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (повторних перевірок = 1)
        dag=my_dag,
    )

    # Встановлення залежностей між завданнями
    create_table >> pick_medal >> pick_medal_task >> [calc_Gold, calc_Silver, calc_Bronze]
    calc_Gold >> generate_dalay
    calc_Silver >> generate_dalay
    calc_Bronze >> generate_dalay
    generate_dalay >> check_for_correctness