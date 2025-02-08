from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

# Визначення аргументів DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def pick_medal():
    return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])


with DAG(
        'nvi_olympic_medal_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db_nvi',
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal,
    )

    calc_bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='goit_mysql_db_nvi',
        sql="""
        INSERT INTO olympic_medal_counts (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze';
        """,
    )

    calc_silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='goit_mysql_db_nvi',
        sql="""
        INSERT INTO olympic_medal_counts (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver';
        """,
    )

    calc_gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='goit_mysql_db_nvi',
        sql="""
        INSERT INTO olympic_medal_counts (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold';
        """,
    )

    generate_delay_task = TimeDeltaSensor(
        task_id='generate_delay',
        delta=timedelta(seconds=35),
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        mysql_conn_id='goit_mysql_db_nvi',
        sql="""
        SELECT COUNT(*) FROM olympic_medal_counts 
        WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        timeout=60,
        poke_interval=10,
    )

    create_table >> pick_medal_task >> [calc_bronze, calc_silver,
                                        calc_gold] >> generate_delay_task >> check_for_correctness
