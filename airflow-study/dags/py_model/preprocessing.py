from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.utils import resample
import pandas as pd
import logging


def preprocess_datamart(df, target ='order_status_Cancelled', downsample=True):
    df = df.copy()

    dropcols = ['first_name', 'last_name']
    ohecols = ['owns_car', 'deceased_indicator', 'online_order', 'order_status']
    booldict = {'owns_car': 'No', 'deceased_indicator': 'N', 'online_order': '0.0'}
    meancols = ['past_3_years_bike_related_purchases', 'standard_cost', 'tenure',
                'product_first_sold_date']
    labelcols = ['wealth_segment', 'brand', 'job_title', 'job_industry_category',
                 'product_size', 'product_class', 'product_line', 'gender']
    datecols = ['DOB', 'transaction_date']
    idcols = ['transaction_id', 'product_id', 'customer_id'] + labelcols

    logging.info('dropping dropcols')
    df = df.drop(dropcols, axis=1)
    df = process_gender(df)
    df = process_all_dates(df, datecols)
    df = process_conts(df, meancols)
    df = process_for_bools(df, booldict)
    df = process_to_labels(df, labelcols)
    df = convert_to_int32(df, idcols)
    df = pd.get_dummies(df, columns=ohecols, drop_first=True)
    if downsample:
        df = downsampling(df, target)
    return df


def process_to_labels(dfdm, labelcols):
    logging.info('processing to labels')
    for col in labelcols:
        dfdm[col] = dfdm[col].fillna('None')
        label(dfdm, col)
    return dfdm


def convert_to_int32(dfdm, idcols):
    logging.info('converting data')
    for col in idcols:
        dfdm[col] = dfdm[col].astype('int32')
    return dfdm


def process_for_bools(dfdm, booldict):

    logging.info('creating bools')
    for col, na in booldict.items():
        dfdm[col] = dfdm[col].fillna(na)
    return dfdm


def process_conts(dfdm, meancols):

    logging.info('fillnans of continuous data')
    for col in meancols:
        dfdm[col] = dfdm[col].fillna(dfdm[col].mean())
    return dfdm


def process_gender(dfdm):

    logging.info('subsititute vlaues for gender')
    dfdm['gender'] = dfdm['gender'].replace({'Male': 'M', 'Female': 'F', 'Femal': 'F', None: 'U'})
    return dfdm


def label(df, target):

    logging.info('label encode')
    label_encoder = LabelEncoder()
    df[target] = label_encoder.fit_transform(df[target])
    return df


def drop_single(df, target):

    logging.info('drop single values')
    id_counts = df[target].value_counts()
    ids_to_drop = id_counts[id_counts < 2].index.tolist()
    df = df[~df[target].isin(ids_to_drop)]
    return df


def process_all_dates(df, datecols):

    logging.info('process dates')
    for col in datecols:
        df = process_date(df, col)
    return df


def process_date(df, datecol):

    logging.info('process 1 date')

    if df[datecol].dtype == 'O':
        df[datecol] = pd.to_datetime(df[datecol], errors='coerce')
    df[datecol] = df[datecol].fillna(df[datecol].median())

    df[datecol] = pd.to_datetime(df[datecol])
    df[datecol+'_year'] = df[datecol].dt.year
    df[datecol+'_month'] = df[datecol].dt.month
    df[datecol+'_day'] = df[datecol].dt.day
    df[datecol+'_day_of_week'] = df[datecol].dt.dayofweek
    df = df.drop(datecol, axis=1)
    return df


def get_xy(df, target='order_status_Cancelled'):

    logging.info('split into X and target')
    #print(df.columns)
    y = df[target]
    X = df.drop(target, axis=1)
    return X, y


def downsampling(df, target):
    value_counts = df[target].value_counts()
    min_count = value_counts.min()
    downsampled_df = pd.concat([resample(df[df[target] == status],
                                         replace=False,
                                         n_samples=min_count,
                                         random_state=42) for status in value_counts.index])
    return downsampled_df

def split(X, y, size=0.2):
    logging.info('get traintestsplit')
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=size, shuffle=True)
    return X_train, X_test, y_train, y_test

def get_traintest(csvname, target):
    df = pd.read_csv(csvname)
    df_train, df_test = train_test_split(df, test_size=0.1, stratify=df[target], random_state=42)
    return df_train, df_test