# -*- coding: utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = "project_solution_streaming"
FOLDER = "zyaremko_final_fp"  # твоя папка в dags/

with DAG(
    dag_id=DAG_ID,
    description="Final Project — Streaming Pipeline",
    start_date=datetime(2024, 9, 1),
    schedule_interval=None,   # запускається вручну
    catchup=False,
    tags=["final", "streaming", "spark"],
) as dag:

    streaming_task = SparkSubmitOperator(
        task_id="streaming_pipeline",
        conn_id="spark-default",   # конектор до Spark уже є в Airflow
        application=f"dags/{FOLDER}/streaming_pipeline.py",
        verbose=True,
    )
