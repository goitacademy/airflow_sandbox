import random
import time
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.sensors.sql import SqlSensor


# -----------------------
# 1. Вибір медалі
# -----------------------
def choose_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print("Chosen medal:", medal)
    return medal


# -----------------------
# 2. Запис результату
# -----------------------
def insert_medal_count(medal_type):

    hook = MySqlHook(mysql_conn_id="mysql_default")

    query_count = f"""
        SELECT COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal_type}';
    """

    count = hook.get_first(query_count)[0]

    insert_sql = f"""
        INSERT INTO medal_counts (medal_type, count, created_at)
        VALUES ('{medal_type}', {count}, NOW());
    """

    hook.run(insert_sql)
    print(f"[OK] Inserted: {medal_type} = {count}")


# -----------------------
# 3. Branch
# -----------------------
def branch_by_medal(ti):
    medal = ti.xcom_pull(task_ids="choose_medal")
    return f"count_{medal.lower()}"


# -----------------------
# 4. Sleep
# -----------------------
def wait_task():
    time.sleep(10)


# -----------------------
# DAG
# -----------------------

with DAG(
    dag_id="medal_count_dag_clean",
    start_date=pendulum.now().subtract(days=1),
    schedule=None,
    catchup=False,
    tags=["mysql", "branch", "olympics"],
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="mysql_default",
        sql="""
            CREATE TABLE IF NOT EXISTS medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME
            );
        """,
    )

    # 2. Обрати медаль
    choose_medal_task = PythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal,
    )

    # 3. Branch
    branch_task = BranchPythonOperator(
        task_id="branch_medal",
        python_callable=branch_by_medal,
    )

    # 4. Підрахунок
    count_bronze = PythonOperator(
        task_id="count_bronze",
        python_callable=insert_medal_count,
        op_kwargs={"medal_type": "Bronze"},
    )

    count_silver = PythonOperator(
        task_id="count_silver",
        python_callable=insert_medal_count,
        op_kwargs={"medal_type": "Silver"},
    )

    count_gold = PythonOperator(
        task_id="count_gold",
        python_callable=insert_medal_count,
        op_kwargs={"medal_type": "Gold"},
    )

    # 5. Затримка
    wait = PythonOperator(
        task_id="wait",
        python_callable=wait_task,
    )

    # 6. SQL Sensor
    check_recent_record = SqlSensor(
        task_id="check_recent_record",
        conn_id="mysql_default",
        sql="""
            SELECT 1
            FROM medal_counts
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            LIMIT 1;
        """,
        poke_interval=5,
        timeout=60,
        mode="poke",
    )

    # -----------------------
    # Flow
    # -----------------------
    create_table >> choose_medal_task >> branch_task

    branch_task >> count_bronze >> wait
    branch_task >> count_silver >> wait
    branch_task >> count_gold >> wait

    wait >> check_recent_record
