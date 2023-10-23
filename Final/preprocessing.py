from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import pandas as pd


def process_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna()
    df = df.drop(['create_dt', 'update_dt'], axis=1)
    df = process_date(df)
    for col_name in ['product_id', 'customer_id']:
        df[col_name] = df[col_name].astype('int32')

    categorical_columns = ['product_class', 'order_status',
                           'brand', 'product_line', 'product_size']
    df = pd.get_dummies(df, columns=categorical_columns)

    df = label(df, 'online_order')
    # df = drop_single(df, 'customer_id')

    print(df.info())
    return df


def label(df, target):
    label_encoder = LabelEncoder()
    df[target] = label_encoder.fit_transform(df[target])
    return df


def drop_single(df, target):
    id_counts = df[target].value_counts()
    ids_to_drop = id_counts[id_counts < 2].index.tolist()
    df = df[~df[target].isin(ids_to_drop)]
    return df


def process_date(df):
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['year'] = df['transaction_date'].dt.year
    df['month'] = df['transaction_date'].dt.month
    df['day'] = df['transaction_date'].dt.day
    df['day_of_week'] = df['transaction_date'].dt.dayofweek
    df = df.drop('transaction_date', axis=1)
    return df


def get_xy(df, target='customer_id'):
    y = df[target]
    X = df.drop(target, axis=1)
    return X, y


def split(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=True)
    return X_train, X_test, y_train, y_test
