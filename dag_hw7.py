from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import random
import time
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='hw7_olesia_medal_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['hw7', 'medals'],
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='DBneodata',
        sql="""
            CREATE TABLE IF NOT EXISTS medal_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # 2. Випадковий вибір медалі
    def choose_medal():
        return random.choice(['bronze', 'silver', 'gold'])

    choose_medal_task = PythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal,
    )

    # 3. Розгалуження залежно від вибору
    def branch_medal_type(**kwargs):
        ti = kwargs['ti']
        medal = ti.xcom_pull(task_ids='choose_medal')
        return f"{medal}_task"

    branch_task = BranchPythonOperator(
        task_id='branch_medal',
        python_callable=branch_medal_type,
        provide_context=True,
    )

    # 4. Завдання для кожного типу медалі
    def count_medals(medal_type, **kwargs):
        from airflow.hooks.base import BaseHook
        import mysql.connector

        conn = BaseHook.get_connection('DBneodata')
        connection = mysql.connector.connect(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema,
            port=conn.port
        )

        cursor = connection.cursor()
        query = f"""
            SELECT COUNT(*) FROM olympic_dataset.athlete_event_results
            WHERE medal = '{medal_type.capitalize()}';
        """
        cursor.execute(query)
        count = cursor.fetchone()[0]

        insert_query = f"""
            INSERT INTO medal_summary (medal_type, count)
            VALUES ('{medal_type.capitalize()}', {count});
        """
        cursor.execute(insert_query)
        connection.commit()
        cursor.close()
        connection.close()

    bronze_task = PythonOperator(
        task_id='bronze_task',
        python_callable=count_medals,
        op_kwargs={'medal_type': 'bronze'},
    )

    silver_task = PythonOperator(
        task_id='silver_task',
        python_callable=count_medals,
        op_kwargs={'medal_type': 'silver'},
    )

    gold_task = PythonOperator(
        task_id='gold_task',
        python_callable=count_medals,
        op_kwargs={'medal_type': 'gold'},
    )

    # 5. Затримка виконання
    def delay():
        time.sleep(35)

    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Перевірка останнього запису
    check_recent = SqlSensor(
        task_id='check_recent',
        conn_id='DBneodata',
        sql="""
            SELECT 1 FROM medal_summary
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        mode='poke',
        timeout=60,
        poke_interval=10,
    )

    # Визначення послідовності задач
    create_table >> choose_medal_task >> branch_task
    branch_task >> [bronze_task, silver_task, gold_task] >> delay_task >> check_recent
