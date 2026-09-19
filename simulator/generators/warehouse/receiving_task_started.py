from datetime import timedelta, timezone
import random

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)

EVENT_NAME = "ReceivingTaskStarted"


def _normalize_datetime(value):
    """
    Convert all timestamps into timezone-aware UTC.

    Database has mixed timestamp types:

    timestamp without timezone
    timestamp with timezone

    Python cannot compare both directly.
    """

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )


def _get_task_started_time(task):
    simulation_now = _normalize_datetime(
        get_simulation_now()
    )

    candidates = [

        _normalize_datetime(
            task.get("assigned_at")
        ),

        _normalize_datetime(
            task.get("created_at")
        ),

        simulation_now

    ]

    candidates = [
        x for x in candidates
        if x is not None
    ]

    base_time = max(
        candidates
    )

    return (
            base_time
            +
            timedelta(
                minutes=random.randint(
                    5,
                    45
                )
            )
    )


def generate_receiving_task_started():
    with Database() as db:

        # -----------------------------------------
        # Find CREATED receiving task
        # -----------------------------------------

        task = db.fetch_one(
            """
            SELECT *
            FROM warehouse_tasks
            WHERE task_type='RECEIVING'
              AND status='CREATED'
            ORDER BY created_at
            LIMIT 1
            """
        )

        if not task:
            raise Exception(
                "No CREATED receiving task found"
            )

        task_id = task["task_id"]

        shipment_id = task["shipment_id"]

        warehouse_id = task["warehouse_id"]

        worker_id = task["assigned_worker_id"]

        if not worker_id:
            raise Exception(
                f"""
Receiving task has no worker assigned

TASK ID:
{task_id}
"""
            )

        correlation_id = str(
            task["correlation_id"]
        )

        started_at = _get_task_started_time(
            task
        )

        # -----------------------------------------
        # Update task
        # -----------------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks
            SET
                status='STARTED',
                task_started_at=%s,
                started_by=%s
            WHERE task_id=%s
            """,
            (
                started_at,
                worker_id,
                task_id
            )
        )

        # -----------------------------------------
        # Worker busy
        # -----------------------------------------

        db.execute(
            """
            UPDATE workers
            SET
                current_status='BUSY'
            WHERE worker_id=%s
            """,
            (
                worker_id,
            )
        )

        # -----------------------------------------
        # Shipment lifecycle update
        #
        # DELIVERED
        #      |
        #      v
        # RECEIVING
        #
        # -----------------------------------------

        db.execute(
            """
            UPDATE shipments
            SET
                shipment_status='RECEIVING',
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                started_at,
                shipment_id
            )
        )

        payload = {

            "event_type":
                EVENT_NAME,

            "occurred_at":
                started_at.isoformat(),

            "receiving_task":
                {

                    "task_id":
                        task_id,

                    "shipment_id":
                        shipment_id,

                    "warehouse_id":
                        warehouse_id,

                    "worker_id":
                        worker_id,

                    "quantity":
                        task["quantity"],

                    "status":
                        "STARTED",

                    "started_at":
                        started_at.isoformat()

                },

            "correlation_id":
                correlation_id

        }

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="WAREHOUSE_TASK",

            aggregate_id=task_id,

            correlation_id=correlation_id,

            payload=payload
        )

        log_event_success(

            EVENT_NAME,

            {

                "task_id":
                    task_id,

                "shipment_id":
                    shipment_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "started_at":
                    started_at,

                "correlation_id":
                    correlation_id

            }
        )

        print(
            f"""
============================================================
EVENT : {EVENT_NAME}

TASK ID             : {task_id}
SHIPMENT ID         : {shipment_id}
WAREHOUSE ID        : {warehouse_id}
WORKER ID           : {worker_id}

STARTED AT          : {started_at}

CORRELATION ID      : {correlation_id}

STATUS : SUCCESS
============================================================
"""
        )


if __name__ == "__main__":

    try:

        generate_receiving_task_started()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise