from dataclasses import dataclass
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from py_model.preprocessing import get_xy, split
import pickle


@dataclass
class MyModel:
    model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)

    def train_model(self, X_train, y_train):
        self.model.fit(X_train, y_train)
        self.get_accuracy(X_train, y_train, 'Train')
        save_pkl(self.model)
        return self.model

    def predict(self, X):
        y_pred = self.model.predict(X)
        return y_pred

    def get_accuracy(self, X_test, y_test, t='Test'):
        print('predicting', t, '...')
        result = self.predict(X_test)
        accuracy = accuracy_score(y_test, result)
        print(f'{t} accuracy: {accuracy}')
        report = classification_report(y_test, result, output_dict=True)
        print(report)
        return report



    def mock_mainloop(self, df, target='order_status_Cancelled'):
        X, y = get_xy(df, target)
        X_train, X_test, y_train, y_test = split(X, y)
        print('Train result')
        self.train_model(X_train, y_train)
        print('Test result')
        report = self.get_accuracy(X_test, y_test, 'Test')
        return report

def save_pkl(Model_class, name='model.pkl'):
    with open(name, 'wb') as model_file:
        pickle.dump(Model_class, model_file)

def load_pkl(name):
    with open(name, 'rb') as f:
        result = pickle.load(f)
    return result

def model_predict(X,y, model_name='model.pkl', reinit=False):
    # it can reinitialize mlflow model or load the model from pickle
    if not reinit:
        loaded_model = load_pkl(model_name)
    else:
        loaded_model = reinit_model(reinit)
    mymodel = MyModel()
    mymodel.model = loaded_model
    report = mymodel.get_accuracy(X, y)
    return report

def reinit_model(model):
    modclass = MyModel()
    modclass.model = model
    return modclass