from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os


# -------------------- paths --------------------
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
# ------------------------------------------------


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="athletes_etl_pipeline_mds6rdd",
    default_args=default_args,
    description="ETL pipeline: landing -> bronze -> silver -> gold",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mds6rdd"],
) as dag:

    landing_bio = BashOperator(
        task_id="landing_to_bronze_athlete_bio",
        bash_command=(
            f"spark-submit {DAG_DIR}/landing_to_bronze.py athlete_bio"
        ),
    )

    landing_results = BashOperator(
        task_id="landing_to_bronze_athlete_event_results",
        bash_command=(
            f"spark-submit {DAG_DIR}/landing_to_bronze.py athlete_event_results"
        ),
    )

    bronze_to_silver_bio = BashOperator(
        task_id="bronze_to_silver_athlete_bio",
        bash_command=(
            f"spark-submit {DAG_DIR}/bronze_to_silver.py athlete_bio"
        ),
    )

    bronze_to_silver_results = BashOperator(
        task_id="bronze_to_silver_athlete_event_results",
        bash_command=(
            f"spark-submit {DAG_DIR}/bronze_to_silver.py athlete_event_results"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_avg_stats",
        bash_command=(
            f"spark-submit {DAG_DIR}/silver_to_gold.py"
        ),
    )

    landing_bio >> bronze_to_silver_bio
    landing_results >> bronze_to_silver_results
    [bronze_to_silver_bio, bronze_to_silver_results] >> silver_to_gold
