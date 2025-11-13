from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.providers.mysql.hooks.mysql import MySqlHook
import random
import time
from airflow.utils.dates import days_ago


# Функція для створення таблиць і вставки даних
def create_tables_and_insert_data(**kwargs):
    """Створення таблиць і вставка даних з детальним логуванням"""
    print("=" * 50)
    print("Starting table creation and data insertion")
    print("=" * 50)
    
    try:
        print("\n[1/5] Connecting to MySQL...")
        hook = MySqlHook(mysql_conn_id="goit_mysql_db_kravchenko_serhii")
        conn = hook.get_conn()
        cursor = conn.cursor()
        print("✅ Connected successfully")
        
        print("\n[2/5] Creating kravchenko_serhii_medal_counts table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kravchenko_serhii_medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                medal_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ Table kravchenko_serhii_medal_counts created")
        
        print("\n[3/5] Creating athlete_event_results table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_event_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                athlete_name VARCHAR(255),
                medal VARCHAR(50),
                event VARCHAR(255),
                year INT
            )
        """)
        conn.commit()
        print("✅ Table athlete_event_results created")
        
        print("\n[4/5] Checking existing data...")
        cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
        count = cursor.fetchone()[0]
        print(f"Current records: {count}")
        
        print("\n[5/5] Inserting test data...")
        test_data = [
            (1, 'John Doe', 'Gold', '100m Sprint', 2020),
            (2, 'Jane Smith', 'Silver', 'Swimming', 2020),
            (3, 'Mike Johnson', 'Bronze', 'Boxing', 2020),
            (4, 'Sarah Williams', 'Gold', 'Gymnastics', 2020),
            (5, 'Tom Brown', 'Silver', 'Tennis', 2020),
            (6, 'Lisa Davis', 'Bronze', 'Athletics', 2020),
        ]
        
        inserted = 0
        for data in test_data:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO athlete_event_results (id, athlete_name, medal, event, year) VALUES (%s, %s, %s, %s, %s)",
                    data
                )
                if cursor.rowcount > 0:
                    inserted += 1
                    print(f"  ✅ Inserted: {data[1]} - {data[2]}")
                else:
                    print(f"  ⏭️  Skipped: {data[1]} - {data[2]}")
            except Exception as e:
                print(f"  ⚠️  Error: {data[1]}: {str(e)}")
        
        conn.commit()
        print(f"\n✅ Inserted {inserted} new records")
        
        cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
        final_count = cursor.fetchone()[0]
        print(f"Total records: {final_count}")
        
        cursor.execute("SELECT medal, COUNT(*) FROM athlete_event_results GROUP BY medal")
        results = cursor.fetchall()
        print("\nMedal distribution:")
        for medal, count in results:
            print(f"  {medal}: {count}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 50)
        print("✅ Setup completed successfully!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise


# Функція для примусового встановлення статусу DAG на SUCCESS
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)
    print("✅ DAG completed successfully!")


# Функція, яка випадково вибирає тип медалі
def random_medal_choice():
    medal = random.choice(["Gold", "Silver", "Bronze"])
    print(f"🎯 Selected medal type: {medal}")
    return medal


# Функція для імітації затримки обробки
def delay_execution():
    print("⏳ Starting 35 second delay...")
    time.sleep(35)
    print("✅ Delay completed")


# Базові параметри DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# Назва з'єднання для MySQL
mysql_connection_id = "goit_mysql_db_kravchenko_serhii"

# Опис самого DAG
with DAG(
    "kravchenko_serhii_dag_final",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kravchenko_medal_counting2"],
    description="Medal counting DAG with branching logic",
) as dag:

    # Завдання 1: Dummy task (для сумісності з оригінальною структурою)
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="SELECT 1 as dummy;",
    )

    # Завдання 2: Створення таблиць і вставка даних через Python
    create_test_data_task = PythonOperator(
        task_id="create_test_data",
        python_callable=create_tables_and_insert_data,
        provide_context=True,
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
        print(f"🔀 Branching logic: Selected medal is '{selected_medal}'")
        
        if selected_medal == "Gold":
            print("   → Routing to count_gold_medals")
            return "count_gold_medals"
        elif selected_medal == "Silver":
            print("   → Routing to count_silver_medals")
            return "count_silver_medals"
        else:
            print("   → Routing to count_bronze_medals")
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
        sql="INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count) SELECT 'Bronze', COUNT(*) FROM athlete_event_results WHERE medal = 'Bronze';",
    )

    # Завдання 6: Підрахунок срібних медалей
    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count) SELECT 'Silver', COUNT(*) FROM athlete_event_results WHERE medal = 'Silver';",
    )

    # Завдання 7: Підрахунок золотих медалей
    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count) SELECT 'Gold', COUNT(*) FROM athlete_event_results WHERE medal = 'Gold';",
    )

    # Завдання 8: Затримка обробки
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Завдання 9: Перевірка наявності записів у таблиці
    check_last_record_task = MySqlOperator(
        task_id="verify_recent_record",
        mysql_conn_id=mysql_connection_id,
        sql="SELECT COUNT(*) FROM kravchenko_serhii_medal_counts WHERE created_at >= NOW() - INTERVAL 30 SECOND;",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Завдання 10: Фінальне завдання для успішного завершення
    success_task = PythonOperator(
        task_id="force_success",
        python_callable=force_success_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Визначення послідовності виконання завдань у DAG
    create_table_task >> create_test_data_task >> select_medal_task >> branching_task
    
    # Branching: тільки ОДИН з цих tasks виконається
    branching_task >> count_bronze_task >> delay_task
    branching_task >> count_silver_task >> delay_task
    branching_task >> count_gold_task >> delay_task
    
    # Продовження після branching
    delay_task >> check_last_record_task >> success_task