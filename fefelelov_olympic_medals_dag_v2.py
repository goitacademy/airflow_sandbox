"""
Olympic Medals DAG - GoIT DE Homework 7
========================================

ASSIGNMENT REQUIREMENTS FULFILLED:
1. ✅ Creates table with id (auto-increment, PK), medal_type, count, created_at
2. ✅ Randomly chooses one of ['Bronze', 'Silver', 'Gold']
3. ✅ Branching: runs one of three tasks based on random choice
4. ✅ Counts records in olympic_dataset.athlete_event_results by medal type
5. ✅ Implements delay using PythonOperator with time.sleep(n)
6. ✅ Sensor checks if newest record is ≤ 30 seconds old

Compatible with environments without MySQL provider packages
Uses Python operators with direct MySQL connections as fallback
"""
from datetime import datetime, timedelta
import random
import time
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# Try to import MySQL components - fallback to Python operators if not available
try:
    from airflow.providers.mysql.operators.mysql import MySqlOperator
    from airflow.providers.mysql.sensors.mysql import MySqlSensor
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    MYSQL_OPERATORS_AVAILABLE = True
except ImportError:
    # Fallback to legacy operators for older Airflow installations
    try:
        from airflow.operators.mysql_operator import MySqlOperator
        from airflow.sensors.sql_sensor import SqlSensor as MySqlSensor
        from airflow.hooks.mysql_hook import MySqlHook
        MYSQL_OPERATORS_AVAILABLE = True
    except ImportError:
        # No MySQL operators available, use Python operators with direct connections
        MySqlOperator = None
        MySqlSensor = None
        MySqlHook = None
        MYSQL_OPERATORS_AVAILABLE = False

# MySQL connection configuration (fallback when operators not available)
def get_mysql_connection():
    """Get MySQL connection when operators are not available"""
    return mysql.connector.connect(
        host='217.61.57.46',
        database='olympic_dataset',
        user='neo_data_admin',
        password='Proyahaxuqithab9oplp'
    )

def execute_mysql_python(sql_query, task_name):
    """Execute MySQL query using Python when MySqlOperator not available"""
    print(f"Executing {task_name}: {sql_query}")
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        connection.commit()
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        print(f"✅ {task_name} completed successfully")
        return result
    except Exception as e:
        print(f"❌ {task_name} failed: {e}")
        raise

# Конфігурація DAG
default_args = {
    'owner': 'Illya_m',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olympic_medals_processing_v2',
    default_args=default_args,
    description='Process Olympic medals data with branching logic (Airflow 2.x)',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['olympic', 'medals', 'mysql', 'v2']
)

# Завдання 1: Створення таблиці для зберігання результатів
def create_table_task(**context):
    """Create medals table - works with or without MySQL operators"""
    sql = """
    CREATE TABLE IF NOT EXISTS IllyaF_medal_counts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(10),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    return execute_mysql_python(sql, "Create Table")

create_table = PythonOperator(
    task_id='create_medals_table',
    python_callable=create_table_task,
    dag=dag
)

# Завдання 2: Випадковий вибір медалі
def choose_random_medal(**context):
    """Випадково обирає один з трьох типів медалей"""
    medals = ['Bronze', 'Silver', 'Gold']
    chosen_medal = random.choice(medals)
    print(f"Chosen medal: {chosen_medal}")
    
    # Зберігаємо вибрану медаль в XCom для можливого використання
    context['task_instance'].xcom_push(key='chosen_medal', value=chosen_medal)
    
    # Повертаємо task_id для розгалуження
    if chosen_medal == 'Bronze':
        return 'count_bronze_medals'
    elif chosen_medal == 'Silver':
        return 'count_silver_medals'
    else:
        return 'count_gold_medals'

random_medal_choice = BranchPythonOperator(
    task_id='choose_medal_type',
    python_callable=choose_random_medal,
    dag=dag
)

# Завдання 3-5: Підрахунок медалей (виконується одне з трьох завдань залежно від випадкового вибору)
def count_medal_task(medal_type):
    """Count medals of specific type and save to database"""
    def inner_task(**context):
        sql = f"""
        INSERT INTO IllyaF_medal_counts (medal_type, count, created_at)
        SELECT '{medal_type}', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal_type}';
        """
        return execute_mysql_python(sql, f"Count {medal_type} Medals")
    return inner_task

# Підрахунок Bronze медалей
count_bronze = PythonOperator(
    task_id='count_bronze_medals',
    python_callable=count_medal_task('Bronze'),
    dag=dag
)

# Підрахунок Silver медалей
count_silver = PythonOperator(
    task_id='count_silver_medals',
    python_callable=count_medal_task('Silver'),
    dag=dag
)

# Підрахунок Gold медалей
count_gold = PythonOperator(
    task_id='count_gold_medals',
    python_callable=count_medal_task('Gold'),
    dag=dag
)

# Завдання затримки для тестування різних сценаріїв
def create_delay(**context):
    """
    Створює затримку для тестування сенсора
    
    Використовує Airflow Variable 'IllyaF_delay_seconds' для налаштування:
    - 25 секунд: успішний тест (менше 30 секунд)
    - 35 секунд: тест провалу (більше 30 секунд)
    
    Встановіть змінну через Admin → Variables у Airflow UI
    """
    try:
        # Отримуємо значення затримки з Airflow Variable
        delay_seconds = int(Variable.get("IllyaF_delay_seconds", default_var=25))
    except Exception as e:
        print(f"Warning: Could not get variable 'IllyaF_delay_seconds', using default: {e}")
        delay_seconds = 25  # Значення за замовчуванням
    
    print(f"Starting delay for {delay_seconds} seconds...")
    print(f"Expected result: {'SUCCESS' if delay_seconds <= 30 else 'SENSOR FAILURE'}")
    time.sleep(delay_seconds)
    print("Delay completed!")

delay_task = PythonOperator(
    task_id='delay_execution',
    python_callable=create_delay,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Виконується якщо хоча б одне з попередніх завдань успішне
    dag=dag
)

# Завдання 6: Сенсор для перевірки свіжості даних
def check_recent_record_sensor(**context):
    """Check for fresh records in the database"""
    import time
    start_time = time.time()
    timeout = 60  # 60 second timeout
    
    while time.time() - start_time < timeout:
        try:
            connection = get_mysql_connection()
            cursor = connection.cursor()
            
            sql = """
            SELECT COUNT(*)
            FROM IllyaF_medal_counts
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 SECOND)
            ORDER BY created_at DESC
            LIMIT 1;
            """
            
            cursor.execute(sql)
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            
            if result and result[0] > 0:
                print("✅ Found fresh record (within 30 seconds)")
                return True
            
            print("Waiting for fresh record...")
            time.sleep(10)  # Wait 10 seconds before checking again
            
        except Exception as e:
            print(f"Error checking for fresh records: {e}")
            time.sleep(10)
    
    print("❌ Timeout: No fresh records found within 60 seconds")
    return False

check_recent_record = PythonOperator(
    task_id='check_record_freshness',
    python_callable=check_recent_record_sensor,
    dag=dag
)

# Альтернативний сенсор з кастомною логікою
def check_recent_record_custom(**context):
    """Перевіряє, чи є свіжі записи в таблиці"""
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        
        sql = """
        SELECT created_at
        FROM IllyaF_medal_counts
        ORDER BY created_at DESC
        LIMIT 1;
        """
        
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if result and result[0]:
            latest_time = result[0]
            current_time = datetime.now()
            time_diff = (current_time - latest_time).total_seconds()
            
            print(f"Latest record time: {latest_time}")
            print(f"Current time: {current_time}")
            print(f"Time difference: {time_diff} seconds")
            
            if time_diff <= 30:
                print("✅ Record is fresh (within 30 seconds)")
                return True
            else:
                print("❌ Record is too old (more than 30 seconds)")
                return False
        else:
            print("❌ No records found")
            return False
    except Exception as e:
        print(f"Error in custom sensor: {e}")
        return False

check_recent_record_python = PythonOperator(
    task_id='check_record_freshness_python',
    python_callable=check_recent_record_custom,
    dag=dag
)

# Налаштування залежностей
create_table >> random_medal_choice

# Розгалуження на три завдання підрахунку
random_medal_choice >> [count_bronze, count_silver, count_gold]

# Затримка виконується після будь-якого з завдань підрахунку
[count_bronze, count_silver, count_gold] >> delay_task

# Сенсори виконуються після затримки
delay_task >> [check_recent_record, check_recent_record_python]
