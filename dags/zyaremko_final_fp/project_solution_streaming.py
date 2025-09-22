# -*- coding: utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = "project_solution_streaming"
FOLDER = "zyaremko_final_fp"  # твоя папка в dags/

with DAG(
        'zyaremko_streaming_fp',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["zyaremko"]
) as dag:

    streaming_task = SparkSubmitOperator(
        task_id="streaming_pipeline",
        conn_id="spark-default",   # конектор до Spark уже є в Airflow
        application=f"dags/{FOLDER}/streaming_pipeline.py",
        verbose=True,
    )
