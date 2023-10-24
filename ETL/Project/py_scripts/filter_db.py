import os
import pandas as pd

def filter_defaults(df: pd.DataFrame) -> pd.DataFrame:
    if 'default' in df.columns:
        df = df.drop('default', axis=1)
    if 'DOB' in df.columns:
        df['DOB'] = pd.to_datetime(df['DOB'], format='%Y-%m-%d', errors='coerce').astype('str')
    return df


def filter_new_customers_list(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if 'unnamed' in col.lower() or col in ['Rank', 'Value']:
            df = df.drop(col, axis=1)
    return df


def get_start_index(df1: pd.DataFrame, df2: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    idx_start = max(df1.index.max(), df2.index.max()) + 1
    new_df['customer_id'] = range(idx_start, idx_start + len(new_df))
    new_df.set_index('customer_id', inplace=True)
    return new_df


def if_db(db_filename: str) -> None:
    if os.path.exists(db_filename):
        os.remove(db_filename)