from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

MYSQL_CONN_ID = "goit_mysql_db"
SCHEMA_NAME = "alina"
TABLE_NAME = f"{SCHEMA_NAME}.hw_dag_results"
DELAY_SECONDS = 10


def pick_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Selected medal: {medal}")
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")

    if medal == "Bronze":
        return "calc_bronze"
    elif medal == "Silver":
        return "calc_silver"
    else:
        return "calc_gold"


def generate_delay():
    print(f"Sleeping for {DELAY_SECONDS} seconds...")
    time.sleep(DELAY_SECONDS)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 1),
}

with DAG(
    dag_id="alina_hw07_medals_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["alina", "hw07"]
) as dag:

    create_schema = MySqlOperator(
        task_id="create_schema",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"CREATE DATABASE IF NOT EXISTS {SCHEMA_NAME};"
    )

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(20),
            count INT,
            created_at DATETIME
        );
        """
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal
    )

    pick_medal_task_branch = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch
    )

    calc_bronze = MySqlOperator(
        task_id="calc_bronze",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    calc_silver = MySqlOperator(
        task_id="calc_silver",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    calc_gold = MySqlOperator(
        task_id="calc_gold",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    generate_delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        SELECT
            CASE
                WHEN TIMESTAMPDIFF(
                    SECOND,
                    IFNULL(MAX(created_at), '1900-01-01 00:00:00'),
                    NOW()
                ) <= 30 THEN 1
                ELSE 0
            END
        FROM {TABLE_NAME};
        """,
        mode="poke",
        poke_interval=5,
        timeout=20
    )

    create_schema >> create_table >> pick_medal_task >> pick_medal_task_branch
    pick_medal_task_branch >> [calc_bronze, calc_silver, calc_gold]
    calc_bronze >> generate_delay_task
    calc_silver >> generate_delay_task
    calc_gold >> generate_delay_task
    generate_delay_task >> check_for_correctness