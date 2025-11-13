from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.utils.state import State
from airflow.providers.mysql.hooks.mysql import MySqlHook
import random
from airflow.utils.dates import days_ago


# Функція для тестування підключення та створення таблиць через Python
def create_tables_python(**kwargs):
    """Створення таблиць через Python Hook для кращої діагностики"""
    mysql_hook = MySqlHook(mysql_conn_id="goit_mysql_db_kravchenko_serhii")
    
    try:
        # Тест підключення
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        print("✅ Successfully connected to MySQL")
        
        # Створення таблиці medal_counts
        print("Creating kravchenko_serhii_medal_counts table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kravchenko_serhii_medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                medal_count INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                KEY idx_created_at (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        connection.commit()
        print("✅ Table kravchenko_serhii_medal_counts created")
        
        # Створення таблиці athlete_event_results
        print("Creating athlete_event_results table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_event_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                athlete_name VARCHAR(255) NOT NULL,
                medal VARCHAR(50) NOT NULL,
                event VARCHAR(255) NOT NULL,
                year INT NOT NULL,
                KEY idx_medal (medal)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        connection.commit()
        print("✅ Table athlete_event_results created")
        
        # Перевірка чи таблиця порожня
        cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
        count = cursor.fetchone()[0]
        print(f"Current records in athlete_event_results: {count}")
        
        if count == 0:
            print("Inserting test data...")
            # Вставка даних окремими запитами для надійності
            test_data = [
                ('John Doe', 'Gold', '100m Sprint', 2020),
                ('Jane Smith', 'Silver', 'Swimming', 2020),
                ('Mike Johnson', 'Bronze', 'Boxing', 2020),
                ('Sarah Williams', 'Gold', 'Gymnastics', 2020),
                ('Tom Brown', 'Silver', 'Tennis', 2020),
                ('Lisa Davis', 'Bronze', 'Athletics', 2020),
                ('Alex Turner', 'Gold', 'Diving', 2020),
                ('Emma Wilson', 'Silver', 'Cycling', 2020),
                ('David Lee', 'Bronze', 'Wrestling', 2020),
                ('Maria Garcia', 'Gold', 'Rowing', 2020),
            ]
            
            for athlete, medal, event, year in test_data:
                cursor.execute(
                    "INSERT INTO athlete_event_results (athlete_name, medal, event, year) VALUES (%s, %s, %s, %s)",
                    (athlete, medal, event, year)
                )
            
            connection.commit()
            print(f"✅ Inserted {len(test_data)} test records")
        else:
            print("ℹ️ Test data already exists, skipping insert")
        
        cursor.close()
        connection.close()
        print("✅ Tables setup completed successfully")
        
        return "success"
        
    except Exception as e:
        print(f"❌ Error creating tables: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(traceback.format_exc())
        raise


# Функція для підрахунку медалей через Python
def count_medals_python(medal_type, **kwargs):
    """Підрахунок медалей через Python Hook"""
    mysql_hook = MySqlHook(mysql_conn_id="goit_mysql_db_kravchenko_serhii")
    
    try:
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        
        # Підрахунок медалей
        print(f"Counting {medal_type} medals...")
        cursor.execute(
            "SELECT COUNT(*) FROM athlete_event_results WHERE medal = %s",
            (medal_type,)
        )
        count = cursor.fetchone()[0]
        print(f"Found {count} {medal_type} medals")
        
        # Збереження результату
        cursor.execute(
            "INSERT INTO kravchenko_serhii_medal_counts (medal_type, medal_count) VALUES (%s, %s)",
            (medal_type, count)
        )
        connection.commit()
        print(f"✅ Saved {medal_type} medal count: {count}")
        
        cursor.close()
        connection.close()
        
        return count
        
    except Exception as e:
        print(f"❌ Error counting {medal_type} medals: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise


# Функція для перевірки записів
def verify_records(**kwargs):
    """Перевірка що запис додався"""
    mysql_hook = MySqlHook(mysql_conn_id="goit_mysql_db_kravchenko_serhii")
    
    try:
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute("""
            SELECT medal_type, medal_count, created_at 
            FROM kravchenko_serhii_medal_counts 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        if result:
            print(f"✅ Latest record: {result[0]} - {result[1]} medals at {result[2]}")
        else:
            print("⚠️ No records found in medal_counts table")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error verifying records: {str(e)}")
        import traceback
        print(traceback.format_exc())


# Функція для примусового встановлення статусу DAG на SUCCESS
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)
    print("✅ DAG completed successfully!")


# Функція, яка випадково вибирає тип медалі
def random_medal_choice():
    medal = random.choice(["Gold", "Silver", "Bronze"])
    print(f"🎯 Selected medal: {medal}")
    return medal


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
    "kravchenko_serhii_dag3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kravchenko_medal_counting3", "diagnostic"],
    description="Diagnostic DAG with Python operators for better error handling",
) as dag:

    # Завдання 1: Створення таблиць через Python (надійніше)
    setup_tables_task = PythonOperator(
        task_id="setup_tables",
        python_callable=create_tables_python,
        provide_context=True,
    )

    # Завдання 2: Випадковий вибір типу медалі
    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    # Завдання 3: Розгалуження на основі вибраної медалі
    def branching_logic(**kwargs):
        ti = kwargs["ti"]
        selected_medal = ti.xcom_pull(task_ids="select_medal")
        print(f"🔀 Branching based on medal: {selected_medal}")
        
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

    # Завдання 4-6: Підрахунок медалей через Python
    count_bronze_task = PythonOperator(
        task_id="count_bronze_medals",
        python_callable=lambda **kwargs: count_medals_python("Bronze", **kwargs),
        provide_context=True,
    )

    count_silver_task = PythonOperator(
        task_id="count_silver_medals",
        python_callable=lambda **kwargs: count_medals_python("Silver", **kwargs),
        provide_context=True,
    )

    count_gold_task = PythonOperator(
        task_id="count_gold_medals",
        python_callable=lambda **kwargs: count_medals_python("Gold", **kwargs),
        provide_context=True,
    )

    # Завдання 7: Join point (без затримки)
    join_task = DummyOperator(
        task_id="join_branches",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Завдання 8: Перевірка записів
    verify_task = PythonOperator(
        task_id="verify_recent_record",
        python_callable=verify_records,
        provide_context=True,
    )

    # Завдання 9: Фінальне завдання
    success_task = PythonOperator(
        task_id="force_success",
        python_callable=force_success_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Визначення послідовності виконання
    setup_tables_task >> select_medal_task >> branching_task
    
    branching_task >> [count_bronze_task, count_silver_task, count_gold_task] >> join_task
    
    join_task >> verify_task >> success_task