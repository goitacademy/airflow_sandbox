from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

MY_CONN_ID = "goit_mysql_db_sergiy_pavlyshyn"


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
    dag_id="goit_de_hw_07_pavlyshyn",
    start_date=datetime(2024, 8, 4),
    schedule_interval=None,
    catchup=False,
    tags=["sergiy_pavlyshyn"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MY_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS pavlyshyn_hw_dag_results (
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
            INSERT INTO pavlyshyn_hw_dag_results (medal_type, count, created_at)
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
        sql="SELECT COUNT(*) FROM pavlyshyn_hw_dag_results WHERE created_at >= NOW() - INTERVAL 30 SECOND",
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
