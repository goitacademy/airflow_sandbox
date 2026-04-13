from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

DB_CONN_ID = "olympic_dataset"
RESULT_TABLE = "olympic_dataset.maxim_hw_dag_results"


def pick_random_medal() -> str:
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Selected medal: {medal}")
    return medal


def choose_branch(ti) -> str:
    medal = ti.xcom_pull(task_ids="pick_medal")
    mapping = {
        "Bronze": "calc_bronze",
        "Silver": "calc_silver",
        "Gold": "calc_gold",
    }
    return mapping[medal]


def generate_delay(**context) -> None:
    delay_seconds = int(context["params"].get("delay_seconds", 10))
    print(f"Sleeping for {delay_seconds} seconds before sensor check")
    time.sleep(delay_seconds)


with DAG(
    dag_id="maxim_hw07_medal_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={"delay_seconds": 10},
    tags=["homework", "airflow", "mysql", "maxim"],
) as dag:
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=DB_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (
            id INT NOT NULL AUTO_INCREMENT,
            medal_type VARCHAR(10) NOT NULL,
            medal_count INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """,
    )

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_random_medal,
    )

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    calc_bronze = MySqlOperator(
        task_id="calc_bronze",
        mysql_conn_id=DB_CONN_ID,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_silver = MySqlOperator(
        task_id="calc_silver",
        mysql_conn_id=DB_CONN_ID,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_gold = MySqlOperator(
        task_id="calc_gold",
        mysql_conn_id=DB_CONN_ID,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=DB_CONN_ID,
        sql=f"""
        SELECT CASE
            WHEN MAX(created_at) IS NOT NULL
                 AND TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
            THEN 1
            ELSE 0
        END AS is_fresh
        FROM {RESULT_TABLE};
        """,
        poke_interval=5,
        timeout=45,
        mode="poke",
    )

    create_table.set_downstream(pick_medal)
    pick_medal.set_downstream(pick_medal_task)
    pick_medal_task.set_downstream([calc_bronze, calc_silver, calc_gold])
    calc_bronze.set_downstream(delay_task)
    calc_silver.set_downstream(delay_task)
    calc_gold.set_downstream(delay_task)
    delay_task.set_downstream(check_for_correctness)
