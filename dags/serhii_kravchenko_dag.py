from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago


# Функція для примусового встановлення статусу DAG на SUCCESS
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)


# Функція, яка випадково вибирає тип медалі
def random_medal_choice():
    medal = random.choice(["Gold", "Silver", "Bronze"])
    print(f"Selected medal: {medal}")
    return medal


# Функція для імітації затримки обробки
def delay_execution():
    print("Starting delay of 35 seconds...")
    time.sleep(35)
    print("Delay completed")


# Базові параметри DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

# Назва з'єднання для MySQL
mysql_connection_id = "goit_mysql_db_kravchenko_serhii"

# Опис самого DAG
with DAG(
    "kravchenko_serhii_dag2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kravchenko_medal_counting2"],
    description="DAG for counting Olympic medals by type",
) as dag:

    # Завдання 1: Створення таблиці для зберігання даних про медалі
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS kravchenko_serhii_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10) NOT NULL,
            medal_count INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        autocommit=True,
    )

    # Завдання 2: Створення тестової таблиці athlete_event_results
    create_test_data_task = MySqlOperator(
        task_id="create_test_data",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS athlete_event_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            athlete_name VARCHAR(255) NOT NULL,
            medal VARCHAR(50) NOT NULL,
            event VARCHAR(255) NOT NULL,
            year INT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        
        -- Insert test data only if table is empty
        INSERT INTO athlete_event_results (athlete_name, medal, event, year)
        SELECT * FROM (
            SELECT 'John Doe' as athlete_name, 'Gold' as medal, '100m Sprint' as event, 2020 as year
            UNION ALL
            SELECT 'Jane Smith', 'Silver', 'Swimming', 2020
            UNION ALL
            SELECT 'Mike Johnson', 'Bronze', 'Boxing', 2020
            UNION ALL
            SELECT 'Sarah Williams', 'Gold', 'Gymnastics', 2020
            UNION ALL
            SELECT 'Tom Brown', 'Silver', 'Tennis', 2020
            UNION ALL
            SELECT 'Lisa Davis', 'Bronze', 'Athletics', 2020
        ) AS tmp
        WHERE NOT EXISTS (
            SELECT 1 FROM athlete_event_results LIMIT 1
        );
        """,
        autocommit=True,
    )

    # Завдання 3: Випадковий вибір типу медалі
    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    # Завдання 4: Розгалуження на основі вибраної медалі
    def branching_logic(**kwargs):
        ti = kwargs["ti"]
        selected_medal = ti.xcom_pull(task_ids="select_medal")
        print(f"Branching based on medal: {selected_medal}")
        
        if selected_medal == "Gold":
            return "count_gold_medals"
        elif selected_medal == "Silver":
            return "count_silver_medals"
        else:
            return "count_bronze_medals"

    branching_task = BranchPythonOperator(
        task_id="branch_based_on_medal",
        python_callable=branching_logic,
        provide_context=True,
    )

    # Завдання 5: Підрахунок бронзових медалей
    count_bronze_task = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
        INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count)
        SELECT 'Bronze', COUNT(*) as count
        FROM athlete_event_results
        WHERE medal = 'Bronze';
        """,
        autocommit=True,
    )

    # Завдання 6: Підрахунок срібних медалей
    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
        INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count)
        SELECT 'Silver', COUNT(*) as count
        FROM athlete_event_results
        WHERE medal = 'Silver';
        """,
        autocommit=True,
    )

    # Завдання 7: Підрахунок золотих медалей
    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
        INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count)
        SELECT 'Gold', COUNT(*) as count
        FROM athlete_event_results
        WHERE medal = 'Gold';
        """,
        autocommit=True,
    )

    # Завдання 8: Затримка обробки
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Завдання 9: Перевірка наявності записів у таблиці
    check_last_record_task = SqlSensor(
        task_id="verify_recent_record",
        conn_id=mysql_connection_id,
        sql="""
        SELECT COUNT(*) as cnt
        FROM kravchenko_serhii_medal_counts 
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 SECOND);
        """,
        mode="poke",
        poke_interval=10,
        timeout=60,
    )

    # Завдання 10: Фінальне завдання для успішного завершення
    success_task = PythonOperator(
        task_id="force_success",
        python_callable=force_success_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Визначення послідовності виконання завдань у DAG
    [create_table_task, create_test_data_task] >> select_medal_task >> branching_task
    
    branching_task >> [count_bronze_task, count_silver_task, count_gold_task] >> delay_task
    
    delay_task >> check_last_record_task >> success_task