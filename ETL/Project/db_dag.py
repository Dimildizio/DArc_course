from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import subprocess
import logging

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
    print('imma printbhihihihihi')
    logging.info("Start hihihihihihi")
    print('list of files Project')

    current_dir = os.getcwd()
    print(f"Files in current directory:")
    for file in os.listdir(current_dir):
        print(file)
    print('files here')
    for file in os.listdir("/opt/airflow/dags"):
        print(file)
    cmd = "python /opt/airflow/dags/main.py"
    #subprocess.run(cmd, shell=True, check=True)
    os.system(cmd)

    logging.info("End hihihihihi")


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
