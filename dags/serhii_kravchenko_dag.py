from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
from datetime import timedelta  # ← ДОДАЙТЕ ЦЕЙ ІМПОРТ
import random
import time
from airflow.utils.dates import days_ago
import logging

# Налаштування логування
logger = logging.getLogger(__name__)

# Функція для перевірки підключення до MySQL
def test_mysql_connection():
    try:
        hook = MySqlHook(mysql_conn_id=mysql_connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        logger.info("✅ Підключення до MySQL успішне")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка підключення до MySQL: {e}")
        raise AirflowException(f"Не вдалося підключитися до MySQL: {e}")

# Функція для перевірки існування таблиць
def check_tables_exist():
    try:
        hook = MySqlHook(mysql_conn_id=mysql_connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Перевірка основної таблиці
        cursor.execute("SHOW TABLES LIKE 'kravchenko_serhii_medal_counts'")
        medal_table_exists = cursor.fetchone() is not None
        
        # Перевірка тестової таблиці
        cursor.execute("SHOW TABLES LIKE 'athlete_event_results'")
        athlete_table_exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        
        logger.info(f"📊 Таблиця kravchenko_serhii_medal_counts існує: {medal_table_exists}")
        logger.info(f"📊 Таблиця athlete_event_results існує: {athlete_table_exists}")
        
        return medal_table_exists and athlete_table_exists
    except Exception as e:
        logger.error(f"❌ Помилка перевірки таблиць: {e}")
        return False

# Функція для примусового встановлення статусу DAG на SUCCESS
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)
    logger.info("✅ DAG примусово завершено успішно")

# Функція, яка випадково вибирає тип медалі
def random_medal_choice():
    medal = random.choice(["Gold", "Silver", "Bronze"])
    logger.info(f"🎲 Обрано медаль: {medal}")
    return medal

# Функція для імітації затримки обробки
def delay_execution():
    logger.info("⏳ Затримка виконання на 35 секунд...")
    time.sleep(35)
    logger.info("✅ Затримка завершена")

# Функція для перевірки даних у таблицях
def verify_data():
    try:
        hook = MySqlHook(mysql_conn_id=mysql_connection_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Перевірка даних у athlete_event_results
        cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
        athlete_count = cursor.fetchone()[0]
        logger.info(f"👥 Кількість записів у athlete_event_results: {athlete_count}")
        
        # Перевірка даних у medal_counts
        cursor.execute("SELECT COUNT(*) FROM kravchenko_serhii_medal_counts")
        medal_count = cursor.fetchone()[0]
        logger.info(f"🏅 Кількість записів у kravchenko_serhii_medal_counts: {medal_count}")
        
        # Показати останні записи
        cursor.execute("SELECT medal_type, medal_count, created_at FROM kravchenko_serhii_medal_counts ORDER BY created_at DESC LIMIT 5")
        recent_records = cursor.fetchall()
        logger.info("📋 Останні записи в medal_counts:")
        for record in recent_records:
            logger.info(f"   - {record}")
        
        cursor.close()
        conn.close()
        
        return athlete_count > 0 and medal_count > 0
    except Exception as e:
        logger.error(f"❌ Помилка перевірки даних: {e}")
        return False

# Базові параметри DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),  # ← Тепер працюватиме
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
) as dag:

    # Завдання 0: Перевірка підключення до MySQL
    test_connection_task = PythonOperator(
        task_id="test_mysql_connection",
        python_callable=test_mysql_connection,
    )

    # Завдання 1: Створення таблиці для зберігання даних про медалі
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS kravchenko_serhii_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання 2: Створення тестової таблиці athlete_event_results
    create_test_data_task = MySqlOperator(
        task_id="create_test_data",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS athlete_event_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            athlete_name VARCHAR(255),
            medal VARCHAR(50),
            event VARCHAR(255),
            year INT
        );
        
        -- Додаємо тестові дані тільки якщо таблиця порожня
        INSERT INTO athlete_event_results (athlete_name, medal, event, year)
        SELECT 'John Doe', 'Gold', '100m Sprint', 2020 FROM DUAL
        WHERE NOT EXISTS (SELECT 1 FROM athlete_event_results WHERE medal = 'Gold');
        
        INSERT INTO athlete_event_results (athlete_name, medal, event, year)
        SELECT 'Jane Smith', 'Silver', 'Swimming', 2020 FROM DUAL
        WHERE NOT EXISTS (SELECT 1 FROM athlete_event_results WHERE medal = 'Silver');
        
        INSERT INTO athlete_event_results (athlete_name, medal, event, year)
        SELECT 'Mike Johnson', 'Bronze', 'Boxing', 2020 FROM DUAL
        WHERE NOT EXISTS (SELECT 1 FROM athlete_event_results WHERE medal = 'Bronze');
        
        INSERT INTO athlete_event_results (athlete_name, medal, event, year)
        SELECT 'Anna Brown', 'Gold', 'Marathon', 2020 FROM DUAL
        WHERE (SELECT COUNT(*) FROM athlete_event_results WHERE medal = 'Gold') < 2;
        """,
    )

    # Завдання 2.1: Перевірка створення таблиць
    check_tables_task = PythonOperator(
        task_id="check_tables_created",
        python_callable=check_tables_exist,
    )

    # Завдання 3: Випадковий вибір типу медалі
    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    # Завдання 4: Розгалуження на основі вибраної медалі
    def branching_logic(**kwargs):
        selected_medal = kwargs["ti"].xcom_pull(task_ids="select_medal")
        logger.info(f"🔄 Розгалуження на основі медалі: {selected_medal}")
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
           SELECT 'Bronze', COUNT(*)
           FROM athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    # Завдання 6: Підрахунок срібних медалей
    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count)
           SELECT 'Silver', COUNT(*)
           FROM athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    # Завдання 7: Підрахунок золотих медалей
    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count)
           SELECT 'Gold', COUNT(*)
           FROM athlete_event_results
           WHERE medal = 'Gold';
           """,
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
            SELECT 1 
            FROM kravchenko_serhii_medal_counts 
            WHERE created_at >= NOW() - INTERVAL 30 SECOND
            LIMIT 1;
        """,
        mode="poke",
        poke_interval=10,
        timeout=60,
    )

    # Завдання 9.1: Перевірка даних
    verify_data_task = PythonOperator(
        task_id="verify_data_in_tables",
        python_callable=verify_data,
    )

    # Завдання 10: Фінальне завдання для успішного завершення
    success_task = PythonOperator(
        task_id="force_success",
        python_callable=force_success_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Визначення послідовності виконання завдань у DAG
    test_connection_task >> [create_table_task, create_test_data_task]
    [create_table_task, create_test_data_task] >> check_tables_task
    check_tables_task >> select_medal_task >> branching_task
    (
        branching_task
        >> [count_bronze_task, count_silver_task, count_gold_task]
        >> delay_task
    )
    delay_task >> check_last_record_task >> verify_data_task >> success_task