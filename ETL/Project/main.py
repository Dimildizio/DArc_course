import pandas as pd
import sqlite3
from py_model.preprocessing import process_df, preprocess_datamart
from py_model.model import MyModel
from db_process import create_db_processor


if __name__ == '__main__':
    create_db_processor()

    db_loc = 'mydb.db'
    table_names = ['DWH_DATAMART']
    a = ['DWH_FACT_TRANSACTIONS',
                   'DWH_DIM_CUSTOMER_ADDRESSES',
                   'DWH_DIM_CUSTOMERS_DEMOGRAPHIC']

    connection = sqlite3.connect(db_loc)

    dfs = {}
    for name in table_names:
        query = f'SELECT * FROM {name}'
        dfs[name] = pd.read_sql_query(query, connection)
    connection.close()

    #df = process_df(dfs[table_names[0]])
    df = preprocess_datamart(dfs[table_names[0]])

    model = MyModel()
    model.mock_mainloop(df, target='online_order_1.0')
