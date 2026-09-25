from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - Airflow is supplied by the runtime environment
    DAG = None
    PythonOperator = None

from simulator.master_simulator import run_simulation_cycle


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="simulation_trigger_dag",
        description="Trigger a six-hour batch to generate and process future-scheduled supply chain events.",
        schedule_interval="0 */6 * * *",
        start_date=datetime(2026, 9, 20),
        catchup=False,
        max_active_runs=1,
    ) as dag:
        trigger_simulation = PythonOperator(
            task_id="trigger_simulation",
            python_callable=run_simulation_cycle,
        )

        trigger_simulation
