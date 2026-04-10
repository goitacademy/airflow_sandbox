from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


DB_CONN = "DBneodata"
DB_SCHEMA = "daryna_lysenko"
RESULT_TABLE = f"{DB_SCHEMA}.medal_statistics"


def get_random_medal():
    selected = random.choice(["Gold", "Silver", "Bronze"])
    print(f"Medal selected: {selected}")
    return selected


def choose_branch(**kwargs):
    medal_value = kwargs["ti"].xcom_pull(task_ids="select_medal")
    mapping = {
        "Gold": "save_gold_count",
        "Silver": "save_silver_count",
        "Bronze": "save_bronze_count",
    }
    return mapping[medal_value]


def pause_before_check():
    print("Pause for 35 seconds before sensor check")
    time.sleep(35)


with DAG(
    dag_id="olympic_medal_stats_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airflow", "mysql", "homework"],
) as dag:

    init_table = SQLExecuteQueryOperator(
        task_id="init_table",
        conn_id=DB_CONN,
        hook_params={"schema": DB_SCHEMA},
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (
            id INT NOT NULL AUTO_INCREMENT,
            medal_type VARCHAR(10) NOT NULL,
            medal_count INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """,
    )

    select_medal = PythonOperator(
        task_id="select_medal",
        python_callable=get_random_medal,
    )

    route_by_medal = BranchPythonOperator(
        task_id="route_by_medal",
        python_callable=choose_branch,
    )

    save_gold_count = SQLExecuteQueryOperator(
        task_id="save_gold_count",
        conn_id=DB_CONN,
        hook_params={"schema": DB_SCHEMA},
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    save_silver_count = SQLExecuteQueryOperator(
        task_id="save_silver_count",
        conn_id=DB_CONN,
        hook_params={"schema": DB_SCHEMA},
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    save_bronze_count = SQLExecuteQueryOperator(
        task_id="save_bronze_count",
        conn_id=DB_CONN,
        hook_params={"schema": DB_SCHEMA},
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    wait_task = PythonOperator(
        task_id="wait_task",
        python_callable=pause_before_check,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    recent_record_sensor = SqlSensor(
        task_id="recent_record_sensor",
        conn_id=DB_CONN,
        hook_params={"schema": DB_SCHEMA},
        sql=f"""
        SELECT IF(
            MAX(created_at) > NOW() - INTERVAL 30 SECOND,
            1,
            0
        )
        FROM {RESULT_TABLE};
        """,
        poke_interval=5,
        timeout=45,
        mode="poke",
    )

    init_table >> select_medal >> route_by_medal
    (
        route_by_medal
        >> [save_gold_count, save_silver_count, save_bronze_count]
        >> wait_task
        >> recent_record_sensor
    )
