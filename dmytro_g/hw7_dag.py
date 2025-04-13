"""DAG module for calculating medals"""

import random
import time
from datetime import datetime, timedelta

# from airflow.providers.common.sql.sensors.sql import SqlSensor
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow import DAG


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, 0, 0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

NAME = "matajur"
CONNECTION_NAME = f"{NAME}_goit_mysql_db"
DB_SCHEMA = f"{NAME}_olympic_dataset"
DATA_SCHEMA = "olympic_dataset"
MEDALS = ["Bronze", "Silver", "Gold"]

with DAG(
    dag_id=f"{NAME}_medal_processing_dag",
    default_args=DEFAULT_ARGS,
    description="DAG for calculating medals",
    schedule_interval=None,
    catchup=False,
    tags=[NAME, "olimpic_medals"],
) as dag:

    # Task 1: Create table
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONNECTION_NAME,
        sql=f"""
        CREATE DATABASE IF NOT EXISTS {DB_SCHEMA};

        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.medal_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2: Pick medal
    def select_medal():
        """
        Function to randomly choose a medal type
        """
        medal = random.choice(MEDALS)
        print(f"Chosen {medal} medal")
        return medal

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=select_medal,
    )

    # Task 3: Pick medal task
    def select_medal_task(**context):
        """
        Function to choose a medal task basing on selected medal type
        """
        medal = context["task_instance"].xcom_pull(task_ids="pick_medal")

        if medal == "Gold":
            return "calc_Gold"
        if medal == "Silver":
            return "calc_Silver"
        return "calc_Bronze"

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=select_medal_task,
        provide_context=True,
    )

    # Task 4: Medal-specific inserts
    def calculate_medals(medal_type):
        """
        Function to create a medal counting SQL operator
        """
        calc_medals_operator = MySqlOperator(
            task_id=f"calc_{medal_type}",
            mysql_conn_id=CONNECTION_NAME,
            sql=f"""
            INSERT INTO {DB_SCHEMA}.medal_summary (medal_type, count)
            SELECT '{medal_type}', COUNT(*) FROM {DATA_SCHEMA}.athlete_event_results
            WHERE medal = '{medal_type}';
            """,
        )

        return calc_medals_operator

    calc_Bronze = calculate_medals("Bronze")
    calc_Silver = calculate_medals("Silver")
    calc_Gold = calculate_medals("Gold")

    # Task 5: Generate delay
    def make_delay(delay=35):
        """
        Function to generate delay
        """
        print(f"Start {delay}s delay")
        time.sleep(delay)
        print(f"End {delay}s delay")

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=make_delay,
        trigger_rule=tr.ONE_SUCCESS,  # triggers only if the previous task is successfully completed
    )

    # Task 6: Check for correctness
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        mysql_conn_id=CONNECTION_NAME,
        sql=f"""
        SELECT 1 FROM {DB_SCHEMA}.medal_summary
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY created_at DESC LIMIT 1;
        """,
        mode="poke",  # Check mode: periodically check the condition
        poke_interval=5,  # Check every 5 seconds
        timeout=40,  # Timeout after 40 seconds
    )

    # Task 7: DAG closure
    def final_function():
        """
        Function to be triggered at DAG closure
        """
        print("DAG completed successfully")

    end_task = PythonOperator(
        task_id="end_task",
        python_callable=final_function,
        trigger_rule=tr.ALL_DONE,  # triggers when all tasks are done
    )

    # DAG dependencies
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
    check_for_correctness >> end_task
