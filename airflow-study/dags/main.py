import requests
import logging
from py_scripts.xlsx2pd import get_df
from py_scripts.filter_db import *
from py_scripts.create_stg_tables import create_stgs
from py_scripts.run_sql_scripts import wrapper_cursor, wrapper_con, read_scripts, getpath, read_to_pd, log_metrics_to_sql
from py_model.model import MyModel, model_predict, save_pkl, load_pkl
from py_model.preprocessing import preprocess_datamart, get_traintest
import openpyxl
import mlflow


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
    logging.info("writing DWH data mart")
    insert_scripts = getpath(scripts_folder, ('create_DWH_DATAMART.sql',))
    wrapper_cursor(db_filename, read_scripts, insert_scripts, many=False)


def insert2dmart(db_filename, scripts_folder):
    logging.info("writing DWH data mart")
    insert_scripts = getpath(scripts_folder, ('insert_DWH_DATAMART.sql',))
    wrapper_cursor(db_filename, read_scripts, insert_scripts, many=False)


def logparams():
    logging.info('start logging params')
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("random_state", 42)
    logging.info('finish logging params')


def get_metrics(report, name):
    num = report.loc['True', name]
    mlflow.log_metric(name, num)
    logging.info(name+' logged '+str(num))

def logmetrics(report):
    for name in ('precision', 'recall', 'f1-score'):
        get_metrics(report, name)

def preprocess_for_model(db_loc, query):
    """"
    Step 1: preprocess data for model
    """

    df = read_to_pd(db_loc, query)
    df = preprocess_datamart(df, 'order_status_Cancelled')
    logging.info('saving data for model')
    df.to_csv('ready_data.csv', sep=',')


def save_train_test(name='ready_data.csv', target='order_status_Cancelled'):
    """
    Step 2: save train test data
    """
    df_train, df_test = get_traintest(name, target)
    df_test.to_csv('test_data.csv', index=False)
    df_train.to_csv('train_data.csv', index=False)


def train_model(name='train_data.csv'):
    """
    Step 3: Train model
    """
    active_run = mlflow.active_run()
    print(active_run)
    logging.info('start mlflow')

    df = pd.read_csv(name)
    logging.info('create model')
    model = MyModel()
    logging.info('log data')
    logparams()
    logging.info('start process')
    model.mock_mainloop(df, target='order_status_Cancelled')
    savemodel(model)
    logging.info('finish training')
    report = predict(name)
    t = str(type(report))
    logging.info(f'REPORT TYPE GIUHOJPDFCGHJFCHGVJKL {t}')
    savemodel(model)
    save_pkl(report, 'report.pkl')
    return report


def save_report(name='report.pkl'):
    """
    Steps 4 and 7: Log all metrics and load them to SQL
    """
    log_all_metrics(name)


def log_all_metrics(name):
    logging.info('loading report')
    report = load_pkl(name)
    logging.info('logging metrics')
    print(report)
    logmetrics(report)
    log_metrics_to_sql(report)
    logging.info(report)
    print(report)


def savemodel(model):
        """
        Step 5: Save model to Model Registry
        """
        #save_pkl(model)
        model_name = 'OrderStatusModel'
        mlflow.sklearn.log_model(model.model, "rf_model")
        model_uri = mlflow.get_artifact_uri()

        #latest_version = mlflow.registered_model.get_latest_versions(name=model_name)
        #model_uri = mlflow.sklearn.get_model_uri("rf_model")
        #mlflow.registered_model.create_registered_model(model_name)
        mlflow.register_model(model_uri, model_name)
        #model = mlflow.registered_model.get_model_version(name=model_name, version=1)
        #mlflow.registered_model.transition_model_version_stage(name=model_name, stage="Production")


def load_model(model_uri="models:/OrderStatusModel/latest"):
    model = mlflow.sklearn.load_model(model_uri=model_uri)
    return model


def predict(name='test_data.csv', target='order_status_Cancelled'):
    """
    Step 6: Predict test data
    """
    df = pd.read_csv(name)
    X = df.drop(target, axis=1)
    y = df[target]

    logging.info('loading model')
    result = model_predict(X, y)
    report_df = pd.DataFrame(result).transpose()
    return report_df


def run_model(db_name, query):
    """
    mock func for airflow and mlflow.
    # each call here emulates a task in  DAGs
    """
    with mlflow.start_run() as run:
        preprocess_for_model(db_name, query)
        save_train_test()
        train_report = train_model()
        # savemodel()
        save_report(train_report)
        test_report = predict()
        save_report(test_report)

# run_model('mydb1.db', 'SELECT * FROM DWH_DATAMART')
