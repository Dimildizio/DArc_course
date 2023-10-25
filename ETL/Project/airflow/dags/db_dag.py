from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_main_py():
    os.system("python /Project/main.py")

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple DAG to run main.py',
    schedule_interval=timedelta(days=1),
)

run_this = PythonOperator(
    task_id='run_main_py_task',
    python_callable=run_main_py,
    dag=dag,
)

run_this
