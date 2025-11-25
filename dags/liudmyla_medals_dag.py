from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule as tr



connection_name = "goit_mysql_db_liudmylalash"

# my schema name
schema_name = "olympic_dataset"
table_name = f"{schema_name}.liudmyla_medal_stats"



def choose_medal(ti, **kwargs):
    medal = random.choice(["Bronze", "Silver", "Gold"])
    ti.xcom_push(key="selected_medal", value=medal)
    print("Selected:", medal)


def branch_by_medal(ti, **kwargs):
    medal = ti.xcom_pull(task_ids="choose_medal", key="selected_medal")
    return f"{medal.lower()}_task"


def delay_function():
    # set 35 sec — sensor will fall
    # set 10–20 sec — sensor will pass
    time.sleep(35)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4),
}


with DAG(
    dag_id="medals_branching_mysql",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["mysql", "homework"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    choose_medal_task = PythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal,
        provide_context=True,
    )

    branch_task = BranchPythonOperator(
        task_id="branch",
        python_callable=branch_by_medal,
        provide_context=True,
    )

    bronze_task = MySqlOperator(
        task_id="bronze_task",
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {table_name} (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    silver_task = MySqlOperator(
        task_id="silver_task",
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {table_name} (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    gold_task = MySqlOperator(
        task_id="gold_task",
        mysql_conn_id=connection_name,
        sql=f"""
        INSERT INTO {table_name} (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_function,
        trigger_rule=tr.ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id="check_new_record",
        conn_id=connection_name,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
        FROM {table_name};
        """,
        poke_interval=5,
        timeout=60,
        mode="poke"
    )


    create_table >> choose_medal_task >> branch_task
    branch_task >> [bronze_task, silver_task, gold_task] >> delay_task
    delay_task >> check_recent_record
