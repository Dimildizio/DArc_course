FROM apache/airflow:2.7.2

COPY ./dags/main.py /main.py
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install scikit-learn openpyxl