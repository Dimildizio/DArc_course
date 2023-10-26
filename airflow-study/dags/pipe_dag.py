from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import download_file, parse_excel_sheet

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    'download_and_parse_dag',
    default_args=default_args,
    description='A DAG to download and parse an Excel file',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 26),
    catchup=False
)

with dag:
    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        op_args=['https://github.com/Dimildizio/DArc_course/raw/main/ETL/Project/input_data/99Bikers_Raw_data.xlsx']
    )

    parse_task = PythonOperator(
        task_id='parse_excel',
        python_callable=parse_excel_sheet,
        op_args=['Transactions']
    )

    download_task >> parse_task
