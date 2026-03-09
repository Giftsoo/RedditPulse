FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

COPY requirements.txt /opt/airflow/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt