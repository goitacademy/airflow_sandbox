"""
Фінальний проєкт — Частина 1.
Airflow DAG: shon_fp_streaming_pipeline.

Pipeline:
1. MySQL athlete_event_results -> Kafka topic athlete_event_results
2. Kafka stream + MySQL athlete_bio -> aggregation
3. foreachBatch -> output Kafka topic + MySQL table
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))

SPARK_PACKAGES = ",".join([
    "com.mysql:mysql-connector-j:8.0.32",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
])


with DAG(
    dag_id="shon_fp_streaming_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["final_project", "streaming", "spark", "kafka", "shon"],
    description="Streaming pipeline: MySQL + Kafka + Spark foreachBatch",
) as dag:

    mysql_to_kafka = BashOperator(
        task_id="mysql_to_kafka",
        bash_command=(
            f"spark-submit --master local[2] "
            f"--packages {SPARK_PACKAGES} "
            f"{os.path.join(DAG_DIR, 'mysql_to_kafka.py')}"
        ),
    )

    streaming_pipeline = BashOperator(
        task_id="streaming_pipeline",
        bash_command=(
            f"spark-submit --master local[2] "
            f"--packages {SPARK_PACKAGES} "
            f"{os.path.join(DAG_DIR, 'streaming_pipeline.py')}"
        ),
    )

    mysql_to_kafka >> streaming_pipeline