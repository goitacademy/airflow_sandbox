from datetime import datetime

from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator

CONNECTION_ID = "goit_mysql_db_sergii_prostov"
TABLE_NAME = "neo_data.sergii_prostov_medal_counts"


def show_table():
    hook = MySqlHook(mysql_conn_id=CONNECTION_ID)
    rows = hook.get_records(f"SELECT id, medal_type, count, created_at FROM {TABLE_NAME} ORDER BY id")
    print(f"SELECT * FROM {TABLE_NAME}; -- {len(rows)} rows")
    print(f"{'id':<4} {'medal_type':<12} {'count':<10} {'created_at':<20}")
    print("-" * 55)
    for r in rows:
        print(f"{r[0]:<4} {r[1]:<12} {r[2]:<10} {r[3]}")


with DAG(
    dag_id="sergii_prostov_show_table",
    start_date=datetime(2026, 4, 12),
    schedule_interval=None,
    catchup=False,
    tags=["sergii_prostov"],
) as dag:
    PythonOperator(task_id="show_table", python_callable=show_table)
