FROM apache/airflow:3.0.3-python3.11

USER airflow

COPY ./airflow /opt/airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
