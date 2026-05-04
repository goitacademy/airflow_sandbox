from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor

from mysql_utils import CONNECTION_NAME, SCHEMA, TABLE_NAME
from util import DEFAULT_ARGS

with DAG(
    dag_id="check_for_correctness_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=[SCHEMA],
):
    check_freshness = SqlSensor(
        task_id="check_freshness",
        conn_id=CONNECTION_NAME,
        sql=f"""
            SELECT 1 FROM {SCHEMA}.{TABLE_NAME}
            WHERE TIMESTAMPDIFF(SECOND, updated_at, NOW()) <= 30
            LIMIT 1
        """,
        poke_interval=5,
        timeout=60,
        mode="poke",
    )
