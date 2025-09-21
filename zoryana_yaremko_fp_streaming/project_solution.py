from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="zyaremko_final_streaming_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["zyaremko"],
) as dag:

    step1 = BashOperator(
        task_id="step1_read_mysql",
        bash_command="spark-submit /opt/airflow/dags/zoryana_yaremko_fp_streaming/step1_read_mysql.py"
    )

    step2 = BashOperator(
        task_id="step2_kafka_produce_results",
        bash_command="spark-submit /opt/airflow/dags/zoryana_yaremko_fp_streaming/step2_kafka_produce_results.py"
    )

    step3 = BashOperator(
        task_id="step3_kafka_stream_join_write",
        bash_command="spark-submit /opt/airflow/dags/zoryana_yaremko_fp_streaming/step3_kafka_stream_join_write.py"
    )

    step1 >> step2 >> step3
