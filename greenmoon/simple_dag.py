from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

 def show_current_directory():
        cwd = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        print(f"Current Working Directory: {cwd}")

# Function that will call our external Python script
def run_external_script(**kwargs):
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'my_script.py')
    result = subprocess.run(['python', script_path], capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Script execution failed with error: {result.stderr}")
        raise Exception("External script execution failed")

    print(f"Script output: {result.stdout}")
    return result.stdout

# Create the DAG
with DAG(
    'simple_external_script_dag',
    default_args=default_args,
    description='A simple DAG to run an external Python script',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 12),
    catchup=False,
) as dag:

    # Task to run the external script
    run_script_task = PythonOperator(
        task_id='run_external_script',
        python_callable=run_external_script,
    )



    print_cwd = PythonOperator(
                 task_id='print_current_working_directory',
                 python_callable=show_current_directory

    )


    # The task dependency is simple in this case - just one task
    print_cwd
    run_script_task
