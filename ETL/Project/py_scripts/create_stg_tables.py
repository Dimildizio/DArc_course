import pandas as pd


def create_stgs(conn, df_transactions: pd.Dataframe, df_customers_demographics: pd.Dataframe,
                df_new_customers_list: pd.Dataframe, df_customer_addresses: pd.Dataframe) -> None:
    '''Create 4 STG tables in database'''
    df_transactions.to_sql('STG_TRANSACTIONS', conn, if_exists='replace')
    df_customers_demographics.to_sql('STG_CUSTOMERS_DEMOGRAPHICS', conn, if_exists='replace')
    df_new_customers_list.to_sql('STG_NEW_CUSTOMERS_LIST', conn, if_exists='replace')
    df_customer_addresses.to_sql('STG_CUSTOMER_ADDRESS', conn, if_exists='replace')


def check_duplicates_stg(conn):
    '''
    Function for debug to check if there are any duplicates in two df before creating primary keys
    '''

    # Fetch data from STG_CUSTOMER_ADDRESS and STG_NEW_CUSTOMERS_LIST
    query = "SELECT customer_id, address, postcode, state, country, property_valuation FROM STG_CUSTOMER_ADDRESS"
    df_customer_address_test = pd.read_sql(query, conn)

    query = "SELECT customer_id, address, postcode, state, country, property_valuation FROM STG_NEW_CUSTOMERS_LIST"
    df_new_customers_list_test = pd.read_sql(query, conn)

    df_combined = pd.concat([df_customer_address_test, df_new_customers_list_test])

    # Check for duplicate customer_id values
    duplicate_customer_ids = df_combined[df_combined.duplicated(subset=['customer_id'], keep=False)]

    if duplicate_customer_ids.empty:
        print("All customer_id values are unique.")
    else:
        print("Duplicate customer_id values:")
        print(duplicate_customer_ids)
