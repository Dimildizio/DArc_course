import pandas as pd
import sqlite3
from preprocessing import process_df
from model import MyModel


if __name__ == '__main__':
    db_loc = '../ETL/Project/mydb.db'
    table_names = ['DWH_FACT_TRANSACTIONS',
                   'DWH_DIM_CUSTOMER_ADDRESSES',
                   'DWH_DIM_CUSTOMERS_DEMOGRAPHIC']

    connection = sqlite3.connect(db_loc)

    dfs = {}
    for name in table_names:
        query = f'SELECT * FROM {name}'
        dfs[name] = pd.read_sql_query(query, connection)
    connection.close()

    df = process_df(dfs[table_names[0]])
    model = MyModel()
    model.mock_mainloop(df, target='product_class')
