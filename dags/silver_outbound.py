import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


LOGGER = logging.getLogger(__name__)
OUTBOUND_PIPELINES = (
    "order",
    "warehouse_task",
    "shipment",
)


def run_outbound_pipelines():
    from spark.framework.spark_session import create_spark
    from spark.jobs.silver_job import run_pipeline

    spark = create_spark(app_name="daemon-supply-chain-silver-outbound")
    LOGGER.info("Created Spark session for outbound Silver pipelines")
    try:
        results = []
        for pipeline_name in OUTBOUND_PIPELINES:
            results.append(run_pipeline(spark, pipeline_name))
        return results
    finally:
        LOGGER.info("Stopping Spark session for outbound Silver pipelines")
        spark.stop()


with DAG(
    dag_id="silver_outbound",
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
    tags=["silver", "outbound"],
) as dag:
    run_silver = PythonOperator(
        task_id="run_outbound_pipelines",
        python_callable=run_outbound_pipelines,
    )
