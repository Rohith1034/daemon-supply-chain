from datetime import timedelta
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


EVENT_NAME = "TaskStarted"


# =====================================================
# TASK TYPE → BUSINESS EVENT
# =====================================================

TASK_START_EVENT_NAMES = {
    "PICKING": "PickingTaskStarted",
    "PACKING": "PackingTaskStarted",
    "RECEIVING": "ReceivingTaskStarted",
    "PUTAWAY": "PutawayTaskStarted",
    "CYCLE_COUNT": "CycleCountTaskStarted"
}


def _get_task_started_time(task):
    """
    Calculate a causally valid task-start timestamp.

    The task's own timeline is authoritative.

    Priority:
        1. assigned_at
        2. created_at
        3. simulation clock

    The start timestamp is always after the task creation
    or assignment timestamp.
    """

    task_assigned_at = task.get(
        "assigned_at"
    )

    task_created_at = task.get(
        "created_at"
    )

    if task_assigned_at is not None:

        base_time = task_assigned_at

    elif task_created_at is not None:

        base_time = task_created_at

    else:

        base_time = get_simulation_now()

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                5,
                60
            )
        )
    )


def _get_event_name(task_type):
    """
    Return the business-specific start event for
    the warehouse task type.

    Unknown task types fall back to the generic
    TaskStarted event.
    """

    return TASK_START_EVENT_NAMES.get(
        task_type,
        EVENT_NAME
    )


def generate_task_started(task_id=None):

    with Database() as db:

        # ------------------------------------
        # Find CREATED warehouse task
        #
        # If task_id is provided:
        #     operate on that exact task.
        #
        # Otherwise:
        #     start the oldest CREATED task.
        # ------------------------------------

        if task_id:

            task = db.fetch_one(
                """
                SELECT *
                FROM warehouse_tasks
                WHERE task_id=%s
                LIMIT 1
                """,
                (
                    task_id,
                )
            )

        else:

            task = db.fetch_one(
                """
                SELECT *
                FROM warehouse_tasks
                WHERE status='CREATED'
                ORDER BY created_at
                LIMIT 1
                """
            )

        if not task:

            if task_id:

                raise Exception(
                    f"Warehouse task not found: {task_id}"
                )

            raise Exception(
                "No CREATED warehouse task found"
            )

        # ------------------------------------
        # Validate task state
        # ------------------------------------

        if task["status"] != "CREATED":

            raise Exception(
                f"""
Warehouse task cannot be started.

TASK:
{task["task_id"]}

TASK TYPE:
{task["task_type"]}

CURRENT STATUS:
{task["status"]}

EXPECTED STATUS:
CREATED
"""
            )

        task_id = task["task_id"]

        worker_id = task["assigned_worker_id"]

        task_type = task["task_type"]

        # ------------------------------------
        # Validate assigned worker
        # ------------------------------------

        if not worker_id:

            raise Exception(
                f"""
Warehouse task has no assigned worker.

TASK:
{task_id}

TASK TYPE:
{task_type}
"""
            )

        # ------------------------------------
        # Determine business event name
        # ------------------------------------

        event_name = _get_event_name(
            task_type
        )

        # ------------------------------------
        # Calculate business timestamp
        # ------------------------------------

        started_at = _get_task_started_time(
            task
        )

        # ------------------------------------
        # Update task status
        # ------------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks
            SET
                status='STARTED',
                task_started_at=%s,
                started_by=%s,
                assigned_at=COALESCE(
                    assigned_at,
                    %s
                )
            WHERE task_id=%s
            """,
            (
                started_at,
                worker_id,
                started_at,
                task_id
            )
        )

        # ------------------------------------
        # Worker becomes BUSY
        # ------------------------------------

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

        # ------------------------------------
        # Payload
        # ------------------------------------

        payload = {
            "eventType":
                event_name,

            "occurredAt":
                started_at.isoformat(),

            "warehouseTask":
            {
                "taskId":
                    task_id,

                "taskType":
                    task_type,

                "warehouseId":
                    task["warehouse_id"],

                "productId":
                    task["product_id"],

                "locationId":
                    task["location"],

                "shipmentId":
                    task["shipment_id"],

                "orderId":
                    task["order_id"],

                "quantity":
                    task["quantity"],

                "workerId":
                    worker_id,

                "status":
                    "STARTED",

                "startedAt":
                    started_at.isoformat()
            },

            "correlationId":
                str(
                    task["correlation_id"]
                )
        }

        # ------------------------------------
        # Publish event
        # ------------------------------------

        publish_event(
            db=db,
            event_type=event_name,
            aggregate_type="WAREHOUSE_TASK",
            aggregate_id=task_id,
            correlation_id=str(
                task["correlation_id"]
            ),
            payload=payload
        )

        # ------------------------------------
        # Event log
        # ------------------------------------

        log_event_success(
            event_name,
            {
                "task_id":
                    task_id,

                "task_type":
                    task_type,

                "warehouse_id":
                    task["warehouse_id"],

                "worker_id":
                    worker_id,

                "status":
                    "STARTED",

                "started_at":
                    started_at,

                "correlation_id":
                    str(
                        task["correlation_id"]
                    )
            }
        )


if __name__ == "__main__":

    try:

        import sys

        if len(sys.argv) > 1:

            generate_task_started(
                sys.argv[1]
            )

        else:

            generate_task_started()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise