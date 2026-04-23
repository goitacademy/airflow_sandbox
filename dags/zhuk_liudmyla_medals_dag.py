"""
ДЗ 7 — Apache Airflow. Автор: Жук Людмила.

Структура 1-в-1 взята зі стилю робочих студентських DAG-ів курсу (зокрема,
dags/goit_de_hw_07_pavlyshyn.py), щоб гарантовано підхопилось Airflow.

Послідовність task-ів:
  1. create_table       — CREATE TABLE IF NOT EXISTS.
  2. pick_medal         — випадково обирає Bronze/Silver/Gold.
  3. pick_medal_task    — BranchPythonOperator → calc_<Medal>.
  4a/4b/4c. calc_*      — INSERT COUNT(*) з olympic_dataset.athlete_event_results.
  5. generate_delay     — time.sleep(35). trigger_rule=ONE_SUCCESS.
  6. check_for_correctness — SqlSensor: запис не старший за 30 сек.
                             При sleep=35 таймаутне → failed (що й треба).
"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

MY_CONN_ID = "goit_mysql_db_zhuk_liudmyla"
RESULTS_TABLE = "zhuk_liudmyla_hw_dag_results"


def _pick_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Обрано медаль: {medal}")
    return medal


def _pick_branch(ti):
    picked_medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{picked_medal}"


def _generate_delay():
    print("Засинаємо на 35 секунд... Сенсор має 'впасти', бо запис застаріє.")
    time.sleep(35)


with DAG(
    dag_id="medal_counts_zhuk_liudmyla",
    start_date=datetime(2024, 8, 4),
    schedule_interval=None,
    catchup=False,
    tags=["zhuk_liudmyla"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MY_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    pick_medal = PythonOperator(task_id="pick_medal", python_callable=_pick_medal)

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=_pick_branch
    )

    def create_calc_task(medal):
        return MySqlOperator(
            task_id=f"calc_{medal}",
            mysql_conn_id=MY_CONN_ID,
            sql=f"""
            INSERT INTO {RESULTS_TABLE} (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = '{medal}';
            """,
        )

    calc_Bronze = create_calc_task("Bronze")
    calc_Silver = create_calc_task("Silver")
    calc_Gold = create_calc_task("Gold")

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=_generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MY_CONN_ID,
        sql=f"SELECT COUNT(*) FROM {RESULTS_TABLE} "
            f"WHERE created_at >= NOW() - INTERVAL 30 SECOND",
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
