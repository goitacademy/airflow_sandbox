from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule as tr
import random
import time
from datetime import datetime
 
connection_name = "goit_mysql_db_Sergii_S"
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}
 
with DAG(
    dag_id="DAG_medal_count",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["Sergii_S"]
) as dag:
    # Create schema task
    create_schema = MySqlOperator(
        task_id="create_schema",
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS Sergii_S;
        """
    )
 
    # Create table task
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS Sergii_S.medal_results (
            id INT PRIMARY KEY AUTO_INCREMENT,
            medal_type VARCHAR(20),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
 
    # Randomly choose a medal type
    def choose_medal():
        return f"calc_{random.choice(['Gold', 'Silver', 'Bronze'])}"
 
    # PythonOperator to pick a medal
    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=choose_medal
    )
 
    # BranchPythonOperator to decide which calculation task to run
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_medal
    )
 
    # Medal calculation tasks
    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO Sergii_S.medal_results (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )
 
    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO Sergii_S.medal_results (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )
 
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO Sergii_S.medal_results (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )
 
    # Delay task
    def delay():
        time.sleep(3)
 
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=delay,
        trigger_rule=tr.ONE_SUCCESS
    )
 
    # Correctness check sensor
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=connection_name,
        sql="""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
        FROM Sergii_S.medal_results;
        """,
        mode="poke",
        poke_interval=5,
        timeout=20
    )
 
    # Set up task dependencies
    create_schema >> create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Gold, calc_Silver, calc_Bronze]
    [calc_Gold, calc_Silver, calc_Bronze] >> generate_delay >> check_for_correctness