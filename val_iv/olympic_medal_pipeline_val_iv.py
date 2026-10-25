from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db_val_iv"


def choose_medal():
    medals = ["Bronze", "Silver", "Gold"]
    selected_medal = random.choice(medals)

    print(f"Selected medal: {selected_medal}")
    return selected_medal


def choose_branch(ti):
    selected_medal = ti.xcom_pull(task_ids='choose_medal')

    if selected_medal == 'Bronze':
        return 'count_bronze'
    elif selected_medal == 'Silver':
        return 'count_silver'
    else:
        return 'count_gold'


DELAY_SECONDS = 10

def delay_execution():
    time.sleep(DELAY_SECONDS)


with DAG(
        'olympic_medal_pipeline_val_iv',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['val_iv']
) as dag:

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS val_iv.medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """
    )

    choose_medal_task = PythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal,
    )

    branch_task = BranchPythonOperator(
        task_id='choose_branch',
        python_callable=choose_branch,
    )

    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO val_iv.medal_counts (
            medal_type,
            count,
            created_at
        )
        SELECT
            'Bronze',
            COUNT(*),
            NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO val_iv.medal_counts (
            medal_type,
            count,
            created_at
        )
        SELECT
            'Silver',
            COUNT(*),
            NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO val_iv.medal_counts (
            medal_type,
            count,
            created_at
        )
        SELECT
            'Gold',
            COUNT(*),
            NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    delay_task = PythonOperator(
        task_id='delay_execution',
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,
        sql="""
        SELECT
            TIMESTAMPDIFF(
                SECOND,
                MAX(created_at),
                NOW()
            ) <= 30
        FROM val_iv.medal_counts;
        """,
        mode='poke',
        poke_interval=5,
        timeout=10,
    )

    create_table >> choose_medal_task >> branch_task

    branch_task >> [count_bronze, count_silver, count_gold]

    [count_bronze, count_silver, count_gold] >> delay_task

    delay_task >> check_recent_record
