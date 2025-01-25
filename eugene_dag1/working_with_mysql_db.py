from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime
import random

# Функція для генерації випадкового числа
def generate_number(ti):
    number = random.randint(1, 100)
    print(f"Generated number: {number}")

    return number

# Функція для перевірки парності числа
def check_even_odd(ti):
    number = ti.xcom_pull(task_ids='generate_number')

    if number % 2 == 0:
        return 'square_task'
    else:
        return 'cube_task'

# Функція для піднесення числа до квадрата
def square_number(ti):
    number = ti.xcom_pull(task_ids='generate_number')
    result = number ** 2

    ti.xcom_push(key='math_result', value=result)
    print(f"{number} squared is {result}")

# Функція для піднесення числа до куба
def cube_number(ti):
    number = ti.xcom_pull(task_ids='generate_number')
    result = number ** 3

    ti.xcom_push(key='math_result', value=result)
    print(f"{number} cubed is {result}")

# Функція для витягування даних з xcom та prints
def final_function(ti):
    original_number = ti.xcom_pull(task_ids='generate_number')
    math_result = ti.xcom_pull(key='math_result')

    print(f"Original value {original_number}, math_result {math_result}")

# Визначення DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

with DAG(
        'even_or_odd_square_or_cube',
        default_args=default_args,
        schedule_interval='*/10 * * * *',
        catchup=False,
        tags=["oleksiy"]
) as dag:
    generate_number_task = PythonOperator(
        task_id='generate_number',
        python_callable=generate_number,
    )

    check_even_odd_task = BranchPythonOperator(
        task_id='check_even_odd',
        python_callable=check_even_odd,
    )

    square_task = PythonOperator(
        task_id='square_task',
        python_callable=square_number,
    )

    cube_task = PythonOperator(
        task_id='cube_task',
        python_callable=cube_number,
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=final_function,
        trigger_rule=tr.ONE_SUCCESS
    )

    # Встановлення залежностей
    generate_number_task >> check_even_odd_task
    check_even_odd_task >> [square_task, cube_task]
    square_task >> end_task
    cube_task >> end_task