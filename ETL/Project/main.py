from py_model.preprocessing import preprocess_datamart
from py_model.model import MyModel
from py_scripts.run_sql_scripts import read_to_pd
from db_process import create_db_processor


if __name__ == '__main__':
    create_db_processor()

    db_loc = 'mydb.db'
    query = 'SELECT * FROM DWH_DATAMART'

    df = read_to_pd(db_loc, query)
    df = preprocess_datamart(df)

    model = MyModel()
    model.mock_mainloop(df, target='online_order_1.0')
