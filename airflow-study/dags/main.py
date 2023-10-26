import pandas as pd
import requests, os
import openpyxl

def download_file(url):
    response = requests.get(url)
    print("Dowloading")
    with open('input_data.xlsx', 'wb') as file:
        file.write(response.content)
    print('downloaded')

def parse_excel_sheet(sheet_name):
    print(os.listdir())
    df = pd.read_excel('input_data.xlsx', sheet_name=sheet_name)
    print("Columns:")
    print(df.columns)

def hello():
    print('hello')
    return 'hello'
