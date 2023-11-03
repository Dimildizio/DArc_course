from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import preprocess_for_model, save_train_test, train_model, save_report, predict
import sklearn


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
    description='A DAG to train and test the model',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 26),
    catchup=False
)

with dag:
    preprocess_data = PythonOperator(
        task_id='preprocess_data_for_model',
        python_callable=preprocess_for_model,
        op_args=['mydb.db', 'SELECT * FROM DWH_DATAMART'],
    )

    split_data = PythonOperator(
        task_id='split_data_train_test',
        python_callable=save_train_test,
        op_args=['ready_data.csv'],
    )

    train_save = PythonOperator(
        task_id='train_and_save_model',
        python_callable=train_model,
        op_args=['train_data.csv'],
    )

    save_train_metrics = PythonOperator(
        task_id='log_train_metrics',
        python_callable=save_report,
    )

    test_save = PythonOperator(
        task_id='predict_test_data',
        python_callable=predict,
        op_args=['test_data.csv'],
    )


    save_test_metrics = PythonOperator(
        task_id='log_test_rewrite_metrics_db',
        python_callable=save_report,
    )


    preprocess_data >> split_data >> train_save >> save_train_metrics >> test_save >> save_test_metrics