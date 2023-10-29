FROM python:3.10

WORKDIR /dags

COPY requirements.txt /dags/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /dags/

ENV DB_NAME=mydb.db
ENV PYTHONPATH=/dags

CMD ["python", "main.py"]