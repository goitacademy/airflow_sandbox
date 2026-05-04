import random

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from mysql_utils import CONNECTION_NAME, SCHEMA, TABLE_NAME
from util import DEFAULT_ARGS, Medal


def get_antient_olympic_discipline() -> str:
    return random.choice(["Discus Throw", "Long Jump", "Javelin Throw"])


def get_antient_olympics_champion_name() -> str:
    return random.choice(["Patroclos", "Achilles", "Sosios", "Phoebos"])


def get_medal() -> str:
    return random.choice(list(Medal))


def push_medal(ti: TaskInstance) -> None:
    champion_name: str | None = ti.xcom_pull(task_ids="get_antient_olympics_champion_name")
    sport: str | None = ti.xcom_pull(task_ids="get_antient_olympic_discipline")
    medal: str | None = ti.xcom_pull(task_ids="get_medal")

    if champion_name is None:
        raise ValueError("Champion name not found in XCom")
    if sport is None:
        raise ValueError("Sport not found in XCom")
    if medal is None:
        raise ValueError("Medal not found in XCom")

    hook = MySqlHook(mysql_conn_id=CONNECTION_NAME)

    existing = hook.get_records(
        f"SELECT id FROM {SCHEMA}.{TABLE_NAME} WHERE champion_name = %s AND sport = %s AND medal_type = %s",
        parameters=(champion_name, sport, medal),
    )

    if existing:
        print(f"Record found. Updating count for {champion_name} in {sport} ({medal})")
        hook.run(
            f"UPDATE {SCHEMA}.{TABLE_NAME} SET count = count + 1 WHERE champion_name = %s AND sport = %s AND medal_type = %s",
            parameters=(champion_name, sport, medal),
        )
    else:
        print(f"No record found. Inserting {medal} for {champion_name} in {sport}")
        hook.run(
            f"INSERT INTO {SCHEMA}.{TABLE_NAME} (champion_name, sport, medal_type, count) VALUES (%s, %s, %s, %s)",
            parameters=(champion_name, sport, medal, 1),
        )


with DAG(
    dag_id="pick_medal_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=[SCHEMA],
):
    get_discipline_tsk = PythonOperator(
        task_id="get_antient_olympic_discipline",
        python_callable=get_antient_olympic_discipline,
    )

    get_champion_tsk = PythonOperator(
        task_id="get_antient_olympics_champion_name",
        python_callable=get_antient_olympics_champion_name,
    )

    get_medal_tsk = PythonOperator(
        task_id="get_medal",
        python_callable=get_medal,
    )

    push_medal_tsk = PythonOperator(
        task_id="push_medal",
        python_callable=push_medal,
    )

    [get_discipline_tsk, get_champion_tsk, get_medal_tsk] >> push_medal_tsk
