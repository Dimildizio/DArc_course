from preprocessing import *
from model import get_model

def mainloop(df):
    df = df.copy()
    df = drop_dubs(df)
    #  X_train, X_test, y1_train, y1_test, y2_train, y2_test = get_train_val(df, 'Depth', 'Width')
    X, y1, y2 = get_xys(df, 'Depth', 'Width')
    model1 = get_model(X, y1)
    model2 = get_model(X, y2, '2')
    return model1, model2
