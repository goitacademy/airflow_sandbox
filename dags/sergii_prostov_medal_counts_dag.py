import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

CONNECTION_ID = "goit_mysql_db_sergii_prostov"
SCHEMA = "neo_data"
TABLE_NAME = f"{SCHEMA}.sergii_prostov_medal_counts"

# Toggle this to demonstrate success (<30s) and failure (>30s) runs of the sensor.
# The assignment asks for 35s to trigger the "failed" scenario.
DELAY_SECONDS = 5


def pick_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Selected medal: {medal}")
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay():
    print(f"Sleeping for {DELAY_SECONDS} seconds...")
    time.sleep(DELAY_SECONDS)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 11),
}

with DAG(
    dag_id="sergii_prostov_medal_counts_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sergii_prostov"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONNECTION_ID,
        sql=[
            f"CREATE DATABASE IF NOT EXISTS {SCHEMA};",
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(20),
                `count` INT,
                created_at DATETIME
            );
            """,
        ],
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    pick_medal_branch = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, `count`, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, `count`, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (medal_type, `count`, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    generate_delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONNECTION_ID,
        sql=f"""
            SELECT CASE
                WHEN MAX(created_at) IS NOT NULL
                     AND TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
                THEN 1
                ELSE 0
            END AS is_fresh
            FROM {TABLE_NAME};
        """,
        mode="poke",
        poke_interval=5,
        timeout=10,
    )

    create_table >> pick_medal_task >> pick_medal_branch
    pick_medal_branch >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_task >> check_for_correctness
