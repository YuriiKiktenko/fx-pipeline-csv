FROM apache/airflow:3.1.3-python3.12

USER airflow

COPY requirements-docker.txt /tmp/requirements-docker.txt
RUN pip install --no-cache-dir -r /tmp/requirements-docker.txt
