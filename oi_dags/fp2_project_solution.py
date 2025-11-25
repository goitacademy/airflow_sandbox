# буде знаходитись Airflow DAG, який послідовно запускатиме всі три файли.

# Для того щоб запустити Spark jobs з Airflow, необхідно:
# помістити файл зі Spark job у репозиторій airflow_sandbox,
# використати оператор SparkSubmitOperator, вказавши шлях відносно кореня репозиторію, імпортувавши його так:
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator.
# spark_submit_task = SparkSubmitOperator(
#     application='dags/oksana/spark_job.py',
#     task_id='spark_submit_job',
#     conn_id='spark-default',
#     verbose=1,
#     dag=dag,
# )

# Тут
# application='dags/oleksiy/spark_job.py' вказує шлях до Python-скрипту (spark_job.py), який виконує Spark-завдання. Цей скрипт знаходиться в директорії dags/oleksiy.
# task_id='spark_submit_job' — унікальний ідентифікатор для цього завдання в DAG (Directed Acyclic Graph) в Airflow. Ідентифікатор називається spark_submit_job.
# conn_id='spark-default' — ідентифікатор з'єднання (connection ID) для підключення до Spark. У цьому випадку використовується з'єднання з ідентифікатором spark-default.
# verbose=1 — рівень докладності виведення логів. Значення 1 означає, що логування буде докладним.
# dag=dag вказує на об'єкт DAG, до якого належить це завдання. Об'єкт dag уже має бути визначений раніше в коді.

# Spark jobs мають бути додані інкрементально, одна за одною, — для того, щоб упевнитись, що вони також правильно функціонують.

# Частина 2. Building an End-to-End Batch Data Lake :
# завдання полягає в побудові трирівневої архітектури обробки даних: від початкового збереження (landing zone),
# через оброблені та очищені дані (bronze/silver), до фінального аналітичного набору (gold).

# Домашнє завдання до теми “Apache Airflow”
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        'oi_fp_airflow',
        default_args=default_args,
        description="End-to-end batch data lake: landing -> bronze -> silver -> gold",
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["oi_fp"]  # Теги для класифікації DAG
) as dag:

    # 1. Landing -> Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="dags/oi_dags/fp2_landing_to_bronze.py",
        conn_id="spark-default",
        verbose=1,
    )

    # 2. Bronze -> Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="dags/oi_dags/fp2_bronze_to_silver.py",
        conn_id="spark-default",
        verbose=1,
    )

    # 3. Silver -> Gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="dags/oi_dags/fp2_silver_to_gold.py",
        conn_id="spark-default",
        verbose=1,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold