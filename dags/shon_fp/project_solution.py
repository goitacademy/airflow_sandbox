"""
Фінальний проєкт — Частина 2.
Airflow DAG: shon_fp_datalake_pipeline.

Послідовно запускає Spark jobs:
  landing_to_bronze → bronze_to_silver → silver_to_gold
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))


with DAG(
    dag_id="shon_fp_datalake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["final_project", "datalake", "spark", "shon"],
    description="Multi-hop Data Lake: landing → bronze → silver → gold",
) as dag:

    # Етап 1. Landing -> Bronze для athlete_bio
    landing_to_bronze_athlete_bio = BashOperator(
        task_id="landing_to_bronze_athlete_bio",
        bash_command=(
            f"spark-submit --master local[2] "
            f"{os.path.join(DAG_DIR, 'landing_to_bronze.py')} athlete_bio"
        ),
    )

    # Етап 1. Landing -> Bronze для athlete_event_results
    landing_to_bronze_athlete_event_results = BashOperator(
        task_id="landing_to_bronze_athlete_event_results",
        bash_command=(
            f"spark-submit --master local[2] "
            f"{os.path.join(DAG_DIR, 'landing_to_bronze.py')} athlete_event_results"
        ),
    )

    # Етап 2. Bronze -> Silver для athlete_bio
    bronze_to_silver_athlete_bio = BashOperator(
        task_id="bronze_to_silver_athlete_bio",
        bash_command=(
            f"spark-submit --master local[2] "
            f"{os.path.join(DAG_DIR, 'bronze_to_silver.py')} athlete_bio"
        ),
    )

    # Етап 2. Bronze -> Silver для athlete_event_results
    bronze_to_silver_athlete_event_results = BashOperator(
        task_id="bronze_to_silver_athlete_event_results",
        bash_command=(
            f"spark-submit --master local[2] "
            f"{os.path.join(DAG_DIR, 'bronze_to_silver.py')} athlete_event_results"
        ),
    )

    # Етап 3. Silver -> Gold avg_stats
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"spark-submit --master local[2] "
            f"{os.path.join(DAG_DIR, 'silver_to_gold.py')}"
        ),
    )

    # Залежності без list >> list, бо ця версія Airflow це не підтримує
    landing_to_bronze_athlete_bio >> bronze_to_silver_athlete_bio
    landing_to_bronze_athlete_event_results >> bronze_to_silver_athlete_event_results

    bronze_to_silver_athlete_bio >> silver_to_gold
    bronze_to_silver_athlete_event_results >> silver_to_gold