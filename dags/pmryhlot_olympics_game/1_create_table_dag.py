import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

from mysql_utils import CONNECTION_NAME, SCHEMA, TABLE_NAME
from util import DEFAULT_ARGS

with DAG(
    "working_with_mysql_db",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=[SCHEMA],
) as dag:
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONNECTION_NAME,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME}
            (
                id            INT PRIMARY KEY AUTO_INCREMENT,
                champion_name VARCHAR(50) NOT NULL,
                sport         VARCHAR(50) NOT NULL,
                medal_type    VARCHAR(10) NOT NULL,
                count         INT NOT NULL DEFAULT 0,
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """,
    )
