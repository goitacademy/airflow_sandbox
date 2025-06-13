from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.sql import SqlSensor
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime
import random
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 1),
}

dag = DAG(
    dag_id='tsygankov_hw-07_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tsygankov_hw-07'],
)

# 1. Створення таблиці
def create_table_fn():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS medals_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
    """)

create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_fn,
    dag=dag,
)

# 2. Збереження поточного часу
def generate_now(ti):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[store_now] {now_str}")
    ti.xcom_push(key='now_str', value=now_str)

store_now = PythonOperator(
    task_id='store_now',
    python_callable=generate_now,
    dag=dag,
)

# 3. Випадковий вибір медалі
def choose_medal(ti):
    choice = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"[pick_medal] Chosen medal: {choice}")
    ti.xcom_push(key='medal', value=choice)

pick_medal = PythonOperator(
    task_id='pick_medal',
    python_callable=choose_medal,
    dag=dag,
)

# 4. Розгалуження
def branch_task(ti):
    medal = ti.xcom_pull(task_ids='pick_medal', key='medal')
    return f'calc_{medal}'

pick_medal_task = BranchPythonOperator(
    task_id='pick_medal_task',
    python_callable=branch_task,
    dag=dag,
)

# 5. Побудова SQL запиту
def build_insert_sql(medal_type, now_str):
    return f"""
        INSERT INTO medals_summary (medal_type, count, created_at)
        SELECT '{medal_type}', COUNT(*), '{now_str}'
        FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal_type}';
    """

# 6. Вставка медалі
def insert_medal(medal_type, **context):
    now_str = context['ti'].xcom_pull(task_ids='store_now', key='now_str')
    sql = build_insert_sql(medal_type, now_str)
    hook = MySqlHook(mysql_conn_id='mysql_default')
    hook.run(sql)
    print(f"[insert_medal] Inserted {medal_type} at {now_str}")

calc_Bronze = PythonOperator(
    task_id='calc_Bronze',
    python_callable=insert_medal,
    op_kwargs={'medal_type': 'Bronze'},
    dag=dag,
)

calc_Silver = PythonOperator(
    task_id='calc_Silver',
    python_callable=insert_medal,
    op_kwargs={'medal_type': 'Silver'},
    dag=dag,
)

calc_Gold = PythonOperator(
    task_id='calc_Gold',
    python_callable=insert_medal,
    op_kwargs={'medal_type': 'Gold'},
    dag=dag,
)

# 7. Затримка
def delay_function():
    seconds = random.randint(5, 40)
    print(f"[delay_function] Sleeping for {seconds} seconds")
    time.sleep(seconds)

generate_delay = PythonOperator(
    task_id='generate_delay',
    python_callable=delay_function,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# 8. Сенсор на свіжість запису
check_for_correctness = SqlSensor(
    task_id='check_for_correctness',
    conn_id='mysql_default',
    sql="""
        SELECT 1 FROM medals_summary
        WHERE created_at = '{{ ti.xcom_pull(task_ids="store_now", key="now_str") }}'
    """,
    timeout=20,
    poke_interval=5,
    mode='poke',
    dag=dag,
)

# Залежності
create_table >> store_now >> pick_medal >> pick_medal_task
pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
[calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
