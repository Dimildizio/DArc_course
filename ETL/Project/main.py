from py_scripts.xlsx2pd import get_df
from py_scripts.filter_db import filter_defaults, filter_new_customers_list, get_start_index, if_db
from py_scripts.create_stg_tables import create_stgs
from py_scripts.run_sql_scripts import wrapper_cursor, wrapper_con, read_scripts, getpath


if __name__ == '__main__':
    # Create filename and db name
    file_path = 'input_data/99Bikers_Raw_data.xlsx'
    db_filename = 'mydb.db'
    if_db(db_filename)

    # Create DataFrames
    df_transactions = get_df(file_path, 'Transactions', indexcol='transaction_id')
    df_customer_demographics = get_df(file_path, 'CustomerDemographic', indexcol='customer_id')
    df_new_customer_list = get_df(file_path, 'NewCustomerList')
    df_customer_addresses = get_df(file_path, 'CustomerAddress', indexcol='customer_id')

    # Filter dataframes. Also, could have filtered empty values IF we had had time
    df_cd = filter_defaults(df_customer_demographics)
    df_ncl = filter_new_customers_list(df_new_customer_list)
    df_ncl = get_start_index(df_cd, df_customer_addresses, df_ncl)

    # Create STG tables
    wrapper_con(db_filename, create_stgs, df_transactions, df_cd, df_ncl, df_customer_addresses)

    # Create DWH tables
    create_scripts = getpath('sql_scripts', ('create_DWH_FACT_TRANSACTIONS.sql',
                                             'create_DWH_DIM_CUSTOMER_ADDRESSES.sql',
                                             'create_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql'))
    wrapper_cursor(db_filename, read_scripts, create_scripts, many=True)

    # Insert values into DWH tables
    insert_scripts = getpath('sql_scripts', ('insert_DWH_FACT_TRANSACTIONS.sql',
                                             'insert_DWH_DIM_CUSTOMER_ADDRESSES.sql',
                                             'insert_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql'))
    wrapper_cursor(db_filename, read_scripts, insert_scripts, many=False)
