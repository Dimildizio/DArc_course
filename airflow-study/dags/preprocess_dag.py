from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import *

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
        task_id='test_parse',
        python_callable=parse_excel_sheet,
        op_args=['Transactions']
    )

    create_db_task = PythonOperator(
        task_id='create_update_database',
        python_callable=create_db,
        op_args=['mydb.db']
    )

    stg_task = PythonOperator(
        task_id='load2stg',
        python_callable=create_stg,
        op_args=['/opt/airflow/dags/input_data.xlsx']
    )

    dwh_create_task = PythonOperator(
        task_id='create_dwh',
        python_callable=create_dwh,
        op_args=['mydb.db', '/opt/airflow/dags/sql_scripts']
    )

    dwh_insert_task = PythonOperator(
        task_id='insert_data_into_dwh',
        python_callable=insert2dwh,
        op_args=['mydb.db', '/opt/airflow/dags/sql_scripts']
    )

    datamart_task = PythonOperator(
        task_id='create_datamart',
        python_callable=produce_datamart,
        op_args=['mydb.db', '/opt/airflow/dags/sql_scripts']
    )

    datamart_put_task = PythonOperator(
        task_id='insert_into_datamart',
        python_callable=insert2dmart,
        op_args=['mydb.db', '/opt/airflow/dags/sql_scripts']
    )


    download_task >> parse_task >> create_db_task >> stg_task >> dwh_create_task >> dwh_insert_task >> datamart_task >> datamart_put_task
