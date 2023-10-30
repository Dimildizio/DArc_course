import pandas as pd
import requests, os
import logging
from py_scripts.xlsx2pd import get_df
from py_scripts.filter_db import *
from py_scripts.create_stg_tables import create_stgs
from py_scripts.run_sql_scripts import wrapper_cursor, wrapper_con, read_scripts, getpath, read_to_pd, report2sql
from py_model.model import MyModel, model_predict, save_model
from py_model.preprocessing import preprocess_datamart, get_traintest
import openpyxl

def download_file(url):
    response = requests.get(url)
    print("Downloading")
    with open('input_data_.xlsx', 'wb') as file:
        file.write(response.content)
    print('downloaded')

def parse_excel_sheet(sheet_name):
    print(os.listdir())
    dag_folder = '/opt/airflow/dags'  # Adjust this path based on your setup
    print(os.listdir(dag_folder))
    df = pd.read_excel(dag_folder+'/input_data.xlsx', sheet_name=sheet_name)
    print("Columns:")
    print(df.columns)


def create_db(db_name):
    # Create filename and db name
    logging.info("create db")
    if_db(db_name)
    logging.info("db created")

def create_stg(file_path):
    logging.info("Starting creating stg")
    # Create DataFrames
    df_transactions = get_df(file_path, 'Transactions', indexcol='transaction_id')
    df_customer_demographics = get_df(file_path, 'CustomerDemographic', indexcol='customer_id')
    df_new_customer_list = get_df(file_path, 'NewCustomerList')
    df_customer_addresses = get_df(file_path, 'CustomerAddress', indexcol='customer_id')

    logging.info("Filtering sql")
    # Filter dataframes. Also, could have filtered empty values IF we had had time
    df_cd = filter_defaults(df_customer_demographics)
    df_ncl = filter_new_customers_list(df_new_customer_list)
    df_ncl = get_start_index(df_cd, df_customer_addresses, df_ncl)

    logging.info('Writing STGS')
    wrapper_con('mydb.db', create_stgs, df_transactions, df_cd, df_ncl, df_customer_addresses)

def create_dwh(db_filename, scripts_folder):
    logging.info("creating DWHS")
    # Create DWH tables
    create_scripts = getpath(scripts_folder, ('create_DWH_FACT_TRANSACTIONS.sql',
                                             'create_DWH_DIM_CUSTOMER_ADDRESSES.sql',
                                             'create_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql'))
    wrapper_cursor(db_filename, read_scripts, create_scripts, many=True)

def insert2dwh(db_filename, scripts_folder):
    logging.info("writing DWHS")
    insert_scripts = getpath(scripts_folder, ('insert_DWH_FACT_TRANSACTIONS.sql',
                                             'insert_DWH_DIM_CUSTOMER_ADDRESSES.sql',
                                             'insert_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql'))
    wrapper_cursor(db_filename, read_scripts, insert_scripts, many=False)


def produce_datamart(db_filename, scripts_folder):
    logging.info("writing DWHS")
    insert_scripts = getpath(scripts_folder, ('insert_DWH_FACT_TRANSACTIONS.sql',
                                             'insert_DWH_DIM_CUSTOMER_ADDRESSES.sql',
                                             'insert_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql'))
    wrapper_cursor(db_filename, read_scripts, insert_scripts, many=False)


def preprocess_for_model(db_loc, query):
        #with mlflow.start_run() as run:
        # Log parameters, metrics, etc.
        df = read_to_pd(db_loc, query)
        print(df['order_status'].value_counts())
        df = preprocess_datamart(df, 'order_status_Cancelled')
        logging.info('saving data for model')
        df.to_csv('ready_data.csv', sep=',')


def save_train_test(name='ready_data.csv', target='order_status_Cancelled'):

    df_train, df_test = get_traintest(name, target)
    df_test.to_csv('test_data.csv', index=False)
    df_train.to_csv('train_data.csv', index=False)


def train_model(name = 'train_data.csv'):
        df = pd.read_csv(name)
        model = MyModel()
        #mlflow.log_param("n_estimators", 100)
        #mlflow.log_param("max_depth", 5)
        #mlflow.log_param("random_state", 42)

        #train
        model.mock_mainloop(df, target='order_status_Cancelled')
        #mlflow.log_metric("metric_name", metric_value)
        #mlflow.sklearn.log_model(model, "model")
        return model

def predict_model(name = 'test_data.csv', target='order_status_Cancelled'):
    df = pd.read_csv(name)
    X = df.drop(target, axis=1)
    y = df[target]
    result = model_predict(X,y)
    report_df = pd.DataFrame(result).transpose()
    return report_df

def run_model(db_name, query):
    #mock func for airflow and mlflow
    preprocess_for_model(db_name, query)
    save_train_test()
    model = train_model()
    save_model(model)
    report = predict_model()
    report2sql(report)
    print(report)

#run_model('mydb1.db', 'SELECT * FROM DWH_DATAMART')