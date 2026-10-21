FROM apache/airflow:3.3.2

USER airflow

RUN pip install --no-cache-dir \
    "confluent-kafka==2.15.1" \
    "google-cloud-storage==3.13.1"
