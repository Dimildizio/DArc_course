from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.linear_model import LinearRegression
import joblib
import os



def infer(model, X):
    return model.predict(X)


def train(X, y, name):
    model = LinearRegression()
    model.fit(X, y)
    joblib.dump(model, name)
    return model


def test(model, X, y):
    y_hat = infer(model, X)
    rmse = round(mean_squared_error(y_hat, y) ** .5, 10)
    mae = round(mean_absolute_error(y_hat, y), 10)
    print(f'MAE: {mae}\nRMSE: {rmse}')


def get_model(X=False, y=False, name='1'):
    print('current dir is:', os.getcwd())
    folder_name = 'models'
    file_name=f'linear_model_y{name}.plk'
    file_path = os.path.join(folder_name, file_name)
    if os.path.exists(file_path):
        model = joblib.load(file_path)
    else:
        model = train(X, y, file_path)
    return model

