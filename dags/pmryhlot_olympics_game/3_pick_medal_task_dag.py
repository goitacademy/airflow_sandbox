import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from mysql_utils import CONNECTION_NAME, SCHEMA, TABLE_NAME
from util import DEFAULT_ARGS, Medal

MEDAL_COUNT_QUERY: str = f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE_NAME} WHERE medal_type = %s"


def get_medal() -> str:
    return random.choice(list(Medal))


def count_conditional(ti: TaskInstance) -> str:
    medal_type: str = ti.xcom_pull(task_ids="get_medal")
    if medal_type == Medal.BRONZE:
        return "count_bronze_medals"
    if medal_type == Medal.SILVER:
        return "count_silver_medals"
    return "count_gold_medals"


def conditional_success(ti: TaskInstance, dag_run: DagRun) -> None:
    medal: str = ti.xcom_pull(task_ids="get_medal")

    for task_id in ["count_bronze_medals", "count_silver_medals", "count_gold_medals"]:
        result = ti.xcom_pull(task_ids=task_id)
        if result is not None:
            print(f"Total {medal} medals in DB: {result}")
            break

    dag_run.set_state(State.SUCCESS)


def conditional_delay(dag_run: DagRun) -> None:
    if dag_run.state == State.SUCCESS:
        task_delay: int = random.randint(1, 38)
        time.sleep(task_delay)


with DAG(
    dag_id="pick_medal_task_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=[SCHEMA],
):
    get_medal_tsk = PythonOperator(
        task_id="get_medal",
        python_callable=get_medal,
    )

    branch_tsk = BranchPythonOperator(
        task_id="branch_medal_count",
        python_callable=count_conditional,
    )

    count_bronze = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=CONNECTION_NAME,
        sql=MEDAL_COUNT_QUERY,
        parameters=[Medal.BRONZE],
        do_xcom_push=True,
    )

    count_silver = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=CONNECTION_NAME,
        sql=MEDAL_COUNT_QUERY,
        parameters=[Medal.SILVER],
        do_xcom_push=True,
    )

    count_gold = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=CONNECTION_NAME,
        sql=MEDAL_COUNT_QUERY,
        parameters=[Medal.GOLD],
        do_xcom_push=True,
    )

    success_tsk = PythonOperator(
        task_id="conditional_success",
        python_callable=conditional_success,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    delay_tsk = PythonOperator(
        task_id="conditional_delay",
        python_callable=conditional_delay,
    )

    get_medal_tsk >> branch_tsk >> [count_bronze, count_silver, count_gold] >> success_tsk >> delay_tsk
