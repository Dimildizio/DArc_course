import pandas as pd
from sklearn.model_selection import train_test_split


def dataload(filename):
    print(filename)
    return pd.read_csv(filename)


def drop_dubs(df):
    df=df.drop(['IF'], axis=1)
    return df.dropna().drop_duplicates().reset_index()


def get_xys(df, name1, name2):
    df = df.copy()
    target1 = df[name1]
    target2 = df[name2]
    baseline = df.drop([name1, name2, 'index'], axis=1)
    return baseline, target1, target2


def split(df, target):
    return train_test_split(df, target, test_size=0.2, random_state=42)


def get_train_val(df, name1, name2):
    X, y1,y2 = get_xys(df, name1, name2)
    X_train, X_test, y1_train, y1_test = split(df, y1)
    X_train, X_test, y2_train, y2_test = split(df, y2)
    return X_train, X_test, y1_train, y1_test, y2_train, y2_test






