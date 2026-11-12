from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from kafka_consumer.consumer import main_consumer


with DAG(
    dag_id="kafka_consumers",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["kafka", "bronze"],
) as dag:
    consume = PythonOperator(
        task_id="consume_to_bronze",
        python_callable=main_consumer,
        retries=3,
    )
