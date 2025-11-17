from datetime import datetime
from typing import List, Any
import logging
import random
import time


from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator 
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr


# Функція для створення таблиці 'medals' в базі даних MySQL
def _create_table_operator_task(task_id: str, conn_id: str) -> MySqlOperator:
    """
    Creates a MySqlOperator task that creates the 'medals' table
    """
    sql_query = """
    CREATE TABLE IF NOT EXISTS medals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(50) NOT NULL,
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    task = MySqlOperator(
        task_id=task_id,
        mysql_conn_id=conn_id,
        sql=sql_query
    )
    
    return task

# Функція для генерації випадкового значення медалі та запуску відповідного завдання
def _choose_medal_type() -> str:
    """
    Randomly chooses one of three paths (Bronze, Silver, Gold)
    and returns the task_id of the corresponding task.
    """
    chosen_medal = random.choice(['Bronze', 'Silver', 'Gold'])
    logging.info(f"Random choose medal is: {chosen_medal}")
    return chosen_medal
    
# Функція для розгалуження на основі вибраної медалі
def _branching_task_on_medal_choice(**kwargs) -> str:
    """
    Branches the workflow based on the chosen medal type.
    Returns the task_id of the next task to execute.
    """
    ti = kwargs['ti']
    chosen_medal = ti.xcom_pull(task_ids='choose_medal_task')
    logging.info(f"Branching on chosen medal: {chosen_medal}")
    if chosen_medal == 'Bronze':
        return 'count_bronze_task'
    elif chosen_medal == 'Silver':
        return 'count_silver_task'
    else:
        return 'count_gold_task'
    
# Функція для підрахунку медалей певного типу та запису результату в таблицю
def _create_count_insert_operator(medal_type: str) -> MySqlOperator:
    """
    Function that creates a MySqlOperator to count
    and insert the number of medals.
    :param medal_type: Type of medal ('Bronze', 'Silver' or 'Gold')
    :return: object MySqlOperator
    """
    sql_query = """
    INSERT INTO medals (medal_type, count)
    SELECT
        %s,
        COUNT(*)
    FROM
        olympic_dataset.athlete_event_results
    WHERE
        medal = %s;
    """
    sql_parameters: List[Any] = [medal_type, medal_type]
    
    task_id = f'count_{medal_type.lower()}_task'
    
    task = MySqlOperator(
        task_id=task_id,
        mysql_conn_id='mysql_default',
        sql=sql_query,
        parameters=sql_parameters
    )
    
    return task

# Функція для затримки виконання на вказану кількість секунд
def _wait_for_n_seconds(seconds_to_wait):
    """
    Sleeps for the specified number of seconds.
    """
    logging.info(f"Starting a delay of {seconds_to_wait} seconds...")
    time.sleep(seconds_to_wait)
    logging.info("Delay completed.")

# Функція для створення SqlSensor для перевірки "свіжості" даних
def _create_freshness_sensor(
    task_id: str,
    table_name: str,
    conn_id: str = 'mysql_default',
    timestamp_column: str = 'created_at',
    freshness_seconds: int = 30,
    poke_interval: int = 5,
    timeout: int = 90
) -> SqlSensor:
    """
    Creates an SqlSensor that checks if the latest entry in the specified table
    is within the specified freshness threshold.    
    """
    sql_query = f"""
    SELECT 1
    FROM {table_name}
    WHERE TIMESTAMPDIFF(
        SECOND,
        (SELECT MAX({timestamp_column}) FROM {table_name}),
        CURRENT_TIMESTAMP
    ) < %s;
    """
    
    sql_parameters: List[Any] = [freshness_seconds]

    sensor_task = SqlSensor(
        task_id=task_id,
        conn_id=conn_id,
        sql=sql_query,
        parameters=sql_parameters,
        poke_interval=poke_interval,
        timeout=timeout,
    )
    
    return sensor_task

# Параметри DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='oie_olympic_medal_counter_with_sensor_1711',
    default_args=default_args,
    description='DAG to count Olympic medals with branching and freshness sensor',
    start_date=datetime(2025, 11, 15),
    schedule_interval=None,
    catchup=False,
    tags=['mysql', 'branching', 'sensor'],
) as dag:

    # Завдання 1: Створення таблиці
    create_table_task = _create_table_operator_task(
        task_id='create_medals_table',
        conn_id='mysql_default'
    )

    # Завдання 2: Випадково обирає медаль
    choose_medal_task = PythonOperator(
        task_id='choose_medal_task',
        python_callable =_choose_medal_type
    )

    # Завдання 3: Розгалуження
    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable =_branching_task_on_medal_choice
    )

    # Завдання 4: Три окремих завдання для підрахунку
    count_bronze = _create_count_insert_operator(medal_type='Bronze')

    count_silver = _create_count_insert_operator(medal_type='Silver')

    count_gold = _create_count_insert_operator(medal_type='Gold')       
    
    # Завдання 5: Затримка
    delay_35_sec_task = PythonOperator(
        task_id='delay_for_35_seconds',
        python_callable=_wait_for_n_seconds,
        op_kwargs={'seconds_to_wait': 35},
        # Правило запуску: виконати, якщо хоча б один з тасків count_bronze, count_silver, count_gold був успішний
        # два інших будуть 'skipped'
        trigger_rule=tr.ONE_SUCCESS
    )

    # Завдання 6: Сенсор (викликаємо вашу функцію-фабрику)
    check_freshness = _create_freshness_sensor(
        task_id='check_record_freshness',
        table_name='medals',
        freshness_seconds=30, # Сенсор "впаде", бо затримка 35 сек
        timeout=60            # Час очікування сенсора
    )

    # Визначення послідовності виконання завдань
    create_table_task >> choose_medal_task >> branching_task
    branching_task >> [count_bronze, count_silver, count_gold]
    [count_bronze, count_silver, count_gold] >> delay_35_sec_task
    delay_35_sec_task >> check_freshness
