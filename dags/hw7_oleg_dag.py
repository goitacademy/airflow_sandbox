from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


DB_CONN = "goit_mysql_db_oleg"
DB_SCHEMA = "olympic_dataset"
RESULT_TABLE = f"{DB_SCHEMA}.oleg_medal_statistics"


def get_random_medal():
    selected = random.choice(["Gold", "Silver", "Bronze"])
    print(f"Medal selected: {selected}")
    return selected


def choose_branch(ti):
    medal_value = ti.xcom_pull(task_ids="select_medal")
    mapping = {
        "Gold": "save_gold_count",
        "Silver": "save_silver_count",
        "Bronze": "save_bronze_count",
    }
    return mapping[medal_value]


def pause_before_check():
    print("Pause for 10 seconds before sensor check")
    time.sleep(10)
    # для перевірки падіння сенсора:
    # time.sleep(35)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="oleg_medal_stats_dag",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["airflow", "mysql", "homework"],
) as dag:

    init_table = MySqlOperator(
        task_id="init_table",
        mysql_conn_id=DB_CONN,
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

    save_gold_count = MySqlOperator(
        task_id="save_gold_count",
        mysql_conn_id=DB_CONN,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    save_silver_count = MySqlOperator(
        task_id="save_silver_count",
        mysql_conn_id=DB_CONN,
        sql=f"""
        INSERT INTO {RESULT_TABLE} (medal_type, medal_count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    save_bronze_count = MySqlOperator(
        task_id="save_bronze_count",
        mysql_conn_id=DB_CONN,
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

    init_table >> select_medal >> route_by_medal
    route_by_medal >> [save_gold_count, save_silver_count, save_bronze_count]
    [save_gold_count, save_silver_count, save_bronze_count] >> wait_task >> recent_record_sensor