import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


LOGGER = logging.getLogger(__name__)
INBOUND_PIPELINES = (
    "purchase_order",
    "supplier_shipment",
    "inventory",
    "goods_receipt",
)


def run_inbound_pipelines():
    from spark.framework.spark_session import create_spark
    from spark.jobs.silver_job import run_pipeline

    spark = create_spark(app_name="daemon-supply-chain-silver-inbound")
    LOGGER.info("Created Spark session for inbound Silver pipelines")
    try:
        results = []
        for pipeline_name in INBOUND_PIPELINES:
            results.append(run_pipeline(spark, pipeline_name))
        return results
    finally:
        LOGGER.info("Stopping Spark session for inbound Silver pipelines")
        spark.stop()


with DAG(
    dag_id="silver_inbound",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="30 */2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email": os.getenv("AIRFLOW_ALERT_EMAIL"),
        "email_on_failure": bool(os.getenv("AIRFLOW_ALERT_EMAIL")),
    },
    tags=["silver", "inbound"],
) as dag:
    run_silver = PythonOperator(
        task_id="run_inbound_pipelines",
        python_callable=run_inbound_pipelines,
    )
