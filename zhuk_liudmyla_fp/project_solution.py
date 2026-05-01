"""
Part 2 · project_solution
=========================

Airflow DAG, що запускає послідовно три Spark-jobs:

    landing_to_bronze  →  bronze_to_silver  →  silver_to_gold

Файли мають лежати у папці airflow_sandbox/zhuk_liudmyla_fp/ (репо-root,
не в dags/), щоб після rsync вони опинились у /opt/airflow/dags/zhuk_liudmyla_fp/.

⚠️  Використовуємо BashOperator + `spark-submit --master local[*]`, а не
    SparkSubmitOperator. Причина: спільний Spark-кластер курсу
    (spark://217.61.58.159:7077) — нестабільний, executor-и крашаться з
    exit code 1 одразу після старту незалежно від коду. local[*] виконує
    job-и в тому ж JVM-процесі що й драйвер (= Airflow worker), і таким
    чином повністю обходить проблемний кластер. Цей же трюк рекомендують
    у курсовому Slack-чаті для обходу інфраструктурних проблем.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

STUDENT = "zhuk_liudmyla"
SCRIPT_DIR = f"/opt/airflow/dags/{STUDENT}_fp"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4),
}


def spark_local_command(script: str) -> str:
    """Команда для запуску Spark-job локально (без зовнішнього кластера)."""
    return (
        f"cd {SCRIPT_DIR} && "
        f"spark-submit --master local[*] {script}"
    )


with DAG(
    dag_id=f"{STUDENT}_fp_batch_datalake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=[STUDENT, "final_project", "part2"],
) as dag:

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=spark_local_command("landing_to_bronze.py"),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=spark_local_command("bronze_to_silver.py"),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=spark_local_command("silver_to_gold.py"),
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
