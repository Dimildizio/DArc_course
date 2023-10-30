from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import run_model


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mlflow_dag',
    default_args=default_args,
    description='A DAG for MLflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 26),
    catchup=False
)

with dag:
    train_mlflow_model = PythonOperator(
        task_id='train_mlflow_model',
        python_callable=run_model,
        op_args=['mydb.db', 'SELECT * FROM DWH_DATAMART'],
    )
    '''
    log_mlflow_params = PythonOperator(
        task_id='log_mlflow_params',
        python_callable=log_mlflow_parameters, #To be implemented
        op_args=[datamart_task.output],  
    )'''

    train_mlflow_model #>> log_mlflow_params