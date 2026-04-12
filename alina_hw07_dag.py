from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
import random
import time

connection_name = "goit_mysql_db_alina_n"
table_name = "alina_n.alina_hw7_medal_counts"
DELAY_SECONDS = 10


def pick_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Selected medal: {medal}")
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")

    if medal == "Bronze":
        return "calc_Bronze"
    elif medal == "Silver":
        return "calc_Silver"
    else:
        return "calc_Gold"


def generate_delay():
    print(f"Sleeping for {DELAY_SECONDS} seconds...")
    time.sleep(DELAY_SECONDS)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 11),
}

with DAG(
    dag_id="hw_7_alina",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["alina_hw7"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(20),
            `count` INT,
            created_at DATETIME
        );
        """
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {table_name} (medal_type, `count`, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {table_name} (medal_type, `count`, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {table_name} (medal_type, `count`, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    generate_delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=tr.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=connection_name,
        sql=f"""
        SELECT CASE
            WHEN MAX(created_at) IS NOT NULL
                 AND TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
            THEN 1
            ELSE 0
        END AS is_fresh
        FROM {table_name};
        """,
        mode="poke",
        poke_interval=5,
        timeout=10,
    )

    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_task >> check_for_correctness