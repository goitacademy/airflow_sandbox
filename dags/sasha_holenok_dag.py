from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

import random
import time

TABLE_NAME = 'medal_counts'

create_table = MySqlOperator(
    task_id="create_table",
    mysql_conn_id="sasha_holenok_db",
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
)

# 2. Випадковий вибір медалі
def pick_medal():
    return random.choice(['Bronze', 'Silver', 'Gold'])

pick_medal_task = PythonOperator(
    task_id="pick_medal",
    python_callable=pick_medal
)

# 3. Branching
def choose_branch(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')
    return f"calc_{medal}"

branch_task = BranchPythonOperator(
    task_id="pick_medal_task",
    python_callable=choose_branch
)

# 4. Три задачі для підрахунку

calc_gold = MySqlOperator(
    task_id="calc_Gold",
    mysql_conn_id="sasha_holenok_db",
    sql=f"""
    INSERT INTO {TABLE_NAME} (medal_type, count)
    SELECT 'Gold', COUNT(*)
    FROM olympic_dataset.athlete_event_results
    WHERE medal = 'Gold';
    """
)

calc_silver = MySqlOperator(
    task_id="calc_Silver",
    mysql_conn_id="sasha_holenok_db",
    sql=f"""
    INSERT INTO {TABLE_NAME} (medal_type, count)
    SELECT 'Silver', COUNT(*)
    FROM olympic_dataset.athlete_event_results
    WHERE medal = 'Silver';
    """
)

calc_bronze = MySqlOperator(
    task_id="calc_Bronze",
    mysql_conn_id="sasha_holenok_db",
    sql=f"""
    INSERT INTO {TABLE_NAME} (medal_type, count)
    SELECT 'Bronze', COUNT(*)
    FROM olympic_dataset.athlete_event_results
    WHERE medal = 'Bronze';
    """
)

# 5. Delay
def delay():
    time.sleep(35)  # можна змінити на менше для успішного проходження сенсора

generate_delay = PythonOperator(
    task_id="generate_delay",
    python_callable=delay,
    trigger_rule=TriggerRule.ONE_SUCCESS  # важливо!
)

# 6. Сенсор перевірки
check_for_correctness = SqlSensor(
    task_id="check_for_correctness",
    conn_id="sasha_holenok_db",
    sql=f"""
    SELECT 1
    FROM {TABLE_NAME}
    WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
    ORDER BY created_at DESC
    LIMIT 1;
    """,
    poke_interval=5,
    timeout=60
)

# DAG
with DAG(
    dag_id="sasha_holenok_homework_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    create_table >> pick_medal_task >> branch_task

    branch_task >> calc_gold
    branch_task >> calc_silver
    branch_task >> calc_bronze

    [calc_gold, calc_silver, calc_bronze] >> generate_delay >> check_for_correctness