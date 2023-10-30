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


    def mock_mainloop(self, df, target='product_id'):
        X, y = get_xy(df, target)
        X_train, X_test, y_train, y_test = split(X, y)
        print('Train result')
        self.train_model(X_train, y_train)
        print('Test result')
        report = self.get_accuracy(X_test, y_test, 'Test')
        return report

def save_model(Model_class):
    with open('model.pkl', 'wb') as model_file:
        pickle.dump(Model_class, model_file)


def model_predict(X,y, model_name='model.pkl'):
    with open(model_name, 'rb') as model_file:
        loaded_model = pickle.load(model_file)
    report = loaded_model.get_accuracy(X, y)
    return report