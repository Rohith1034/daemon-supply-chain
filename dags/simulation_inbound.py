import os
from uuid import NAMESPACE_URL, uuid5

import psycopg2
import pendulum
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException

API_URL = os.getenv("SIMULATOR_API_URL", "http://simulator-api:8000")
INBOUND_SIMULATION_COUNT = 60


def check_api_status():
    response = requests.get(API_URL)
    if response.status_code != 200:
        raise AirflowFailException("API is not up.❌❌❌")

    print("API is up and running.✅✅✅")


def check_postgres_status():
    required_settings = {
        "dbname": os.getenv("DB_NAME", "supply_chain"),
        "user": os.getenv("DB_USER", "supplychain_app"),
        "password": os.getenv("DB_PASSWORD"),
    }
    missing_settings = [
        name for name, value in required_settings.items() if not value
    ]
    if missing_settings:
        raise AirflowFailException(
            f"Missing PostgreSQL connection settings: {', '.join(missing_settings)}"
        )

    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "host.docker.internal"),
            port=int(os.getenv("DB_PORT", "5432")),
            connect_timeout=5,
            **required_settings,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        finally:
            connection.close()
    except (psycopg2.Error, ValueError) as exc:
        raise AirflowFailException(
            f"PostgreSQL connection failed: {exc}"
        ) from exc

    print("Postgres is up and running ✅✅✅")


def inbound_simulation(**context):
    base_url = API_URL
    flow = "simulation/inbound"
    endpoint = f"{base_url}/{flow}"
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowFailException("Missing Airflow DAG run context")

    for call_number in range(1, INBOUND_SIMULATION_COUNT + 1):
        correlation_id = uuid5(
            NAMESPACE_URL,
            f"{dag_run.dag_id}:{dag_run.run_id}:inbound:{call_number}",
        )
        print(f"Starting inbound simulation {call_number}/60: {endpoint}")
        try:
            response = requests.post(
                endpoint,
                json={"correlation_id": str(correlation_id)},
                timeout=(10, 300),
            )
            response.raise_for_status()
            result = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise AirflowFailException(
                f"Inbound simulation {call_number}/30 request failed: {exc}"
            ) from exc

        if result.get("status") != "SUCCESS":
            print(
                f"Inbound simulation {call_number}/{INBOUND_SIMULATION_COUNT} failed "
                f"(simulation_id={result.get('simulation_id')}): "
                f"{result.get('message', 'API returned a non-success status')}"
            )
            continue

        print(
            f"Inbound simulation {call_number}/{INBOUND_SIMULATION_COUNT} succeeded "
            f"(simulation_id={result.get('simulation_id')}, "
            f"correlation_id={result.get('correlation_id')})"
        )


def removing_invalid_inbound_cycles(**context):
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowFailException("Missing Airflow DAG run context")

    correlation_ids = [
        str(uuid5(
            NAMESPACE_URL,
            f"{dag_run.dag_id}:{dag_run.run_id}:inbound:{call_number}",
        ))
        for call_number in range(1, INBOUND_SIMULATION_COUNT + 1)
    ]

    connection = psycopg2.connect(
        host=os.getenv("DB_HOST", "host.docker.internal"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "supply_chain"),
        user=os.getenv("DB_USER", "supplychain_app"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=5,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT correlation_id
                FROM simulation_runs
                WHERE flow = 'inbound'
                  AND status = 'FAILED'
                  AND correlation_id::text = ANY(%s)
                """,
                (correlation_ids,),
            )
            failed_correlation_ids = [
                str(row[0]) for row in cursor.fetchall()
            ]
            if not failed_correlation_ids:
                print("No failed inbound simulations to clean up.")
                return

            cursor.execute(
                """
                DELETE FROM event_outbox
                WHERE correlation_id = ANY(%s)
                  AND status IN ('PENDING', 'RETRY_SCHEDULED')
                RETURNING event_id
                """,
                (failed_correlation_ids,),
            )
            deleted_events = cursor.fetchall()
        connection.commit()
    finally:
        connection.close()

    print(
        f"Removed {len(deleted_events)} unpublished outbox events "
        f"for {len(failed_correlation_ids)} failed inbound simulations."
    )


with DAG(
        dag_id="simulation_inbound",
        start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Kolkata"),
        schedule="*/10 0,4,8,12,16,20 * * *",
        max_active_runs=1,
        catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="check_api_status",
        python_callable=check_api_status
    )

    t2 = PythonOperator(
        task_id="check_postgres_status",
        python_callable=check_postgres_status
    )

    t3 = PythonOperator(
        task_id="inbound_simulation",
        python_callable=inbound_simulation
    )

    t4 = PythonOperator(
        task_id="clean_invalid_records",
        python_callable=removing_invalid_inbound_cycles,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4
