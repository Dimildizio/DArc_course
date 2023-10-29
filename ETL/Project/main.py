print('12345765432345435234564532test')
import logging
logging.info('importing')

from py_model.preprocessing import preprocess_datamart
from py_model.model import MyModel
from py_scripts.run_sql_scripts import read_to_pd
from db_process import create_db_processor

logging.info("Starting processing")
create_db_processor()


logging.info("Finished processing. Starting predicting")
db_loc = 'mydb.db'
query = 'SELECT * FROM DWH_DATAMART'

df = read_to_pd(db_loc, query)

df.to_csv('datamart_no_prepro.csv', sep=',')
df = preprocess_datamart(df)
df.to_csv('datamart.csv', sep=',')

model = MyModel()
model.mock_mainloop(df, target='online_order_1.0')

logging.info("Finished prediction")