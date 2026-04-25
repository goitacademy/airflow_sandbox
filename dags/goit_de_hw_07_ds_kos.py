import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


CONNECTION_ID = "goit_mysql_db_ds_kos"
DB_SCHEMA = "olympic_dataset"
TABLE_NAME = "ds_kos_medal"
RESULTS_TABLE = f"{DB_SCHEMA}.{TABLE_NAME}"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4),
}


def choose_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"{medal} medal selected")
    return medal


def choose_branch(ti):
    selected_medal = ti.xcom_pull(task_ids="pick_medal")

    if selected_medal == "Bronze":
        return "calc_Bronze"
    elif selected_medal == "Silver":
        return "calc_Silver"
    else:
        return "calc_Gold"


def delay():
    print("Delay started")
    #time.sleep(5)
    time.sleep(35) #check for failed
    print("Delay finished")


with DAG(
    dag_id="ds_kos_medal_branching_hw",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["kostiya"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id INT NOT NULL AUTO_INCREMENT,
            medal_type VARCHAR(10) NOT NULL,
            `count` INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """,
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=choose_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, `count`, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, `count`, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, `count`, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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
        END
        FROM {RESULTS_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=10,
    )

    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_Gold, calc_Silver, calc_Bronze]
    [calc_Gold, calc_Silver, calc_Bronze] >> generate_delay >> check_for_correctness