FROM python:3.10

WORKDIR /Project

COPY requirements.txt /Project/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /Project/

ENV DB_NAME=mydb.db
ENV PYTHONPATH=/Project

CMD ["python", "main.py"]