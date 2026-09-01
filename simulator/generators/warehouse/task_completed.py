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


EVENT_NAME = "TaskCompleted"


def _get_task_completed_time(task, actual_minutes):
    """
    Calculate a causally valid task completion timestamp.

    TaskCompleted must always occur after TaskStarted.

    The primary anchor is task_started_at. The simulation
    clock is considered as a secondary reference so that
    the generated timestamp remains compatible with the
    overall simulation timeline.

    The actual task duration is reflected in the timestamp.
    """

    simulation_now = get_simulation_now()

    task_started_at = task.get(
        "task_started_at"
    )

    assigned_at = task.get(
        "assigned_at"
    )

    created_at = task.get(
        "created_at"
    )

    candidates = [
        candidate
        for candidate in [
            task_started_at,
            assigned_at,
            created_at,
            simulation_now
        ]
        if candidate is not None
    ]

    if not candidates:
        base_time = simulation_now
    else:
        base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=actual_minutes
        )
    )


def generate_task_completed(task_id=None):

    with Database() as db:

        # ---------------------------------
        # Find started warehouse task
        #
        # If task_id is supplied, complete
        # that exact task.
        #
        # Otherwise preserve the current
        # behavior and find the oldest
        # STARTED task.
        # ---------------------------------

        if task_id:

            task = db.fetch_one(
                """
                SELECT *
                FROM warehouse_tasks
                WHERE task_id=%s
                  AND status='STARTED'
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
                WHERE status='STARTED'
                ORDER BY task_started_at
                LIMIT 1
                """
            )

        if not task:

            if task_id:

                raise Exception(
                    f"No STARTED warehouse task found "
                    f"for task_id={task_id}"
                )

            raise Exception(
                "No STARTED warehouse task found"
            )

        task_id = task["task_id"]

        worker_id = task["assigned_worker_id"]

        # ---------------------------------
        # Generate actual task duration
        # ---------------------------------

        actual_minutes = random.randint(
            10,
            45
        )

        # ---------------------------------
        # Calculate causally valid
        # completion timestamp
        # ---------------------------------

        completed_time = _get_task_completed_time(
            task,
            actual_minutes
        )

        accuracy_score = round(
            random.uniform(
                95,
                100
            ),
            2
        )

        productivity_score = round(
            random.uniform(
                85,
                100
            ),
            2
        )

        # ---------------------------------
        # Update warehouse task
        # ---------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks
            SET
                status='COMPLETED',
                actual_minutes=%s,
                completed_at=%s,
                task_completed_at=%s,
                completed_by=%s
            WHERE task_id=%s
            """,
            (
                actual_minutes,
                completed_time,
                completed_time,
                worker_id,
                task_id
            )
        )

        # ---------------------------------
        # Return worker to AVAILABLE
        #
        # The worker is BUSY while working
        # on the task. Once the task completes,
        # the worker becomes available again.
        # ---------------------------------

        if worker_id:

            db.execute(
                """
                UPDATE workers
                SET current_status='AVAILABLE'
                WHERE worker_id=%s
                """,
                (
                    worker_id,
                )
            )

        # ---------------------------------
        # Insert worker productivity
        # ---------------------------------

        db.execute(
            """
            INSERT INTO worker_productivity
            (
                worker_id,
                task_type,
                units_processed,
                working_minutes,
                accuracy_score,
                productivity_score,
                task_id,
                warehouse_id,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                worker_id,
                task["task_type"],
                task["quantity"],
                actual_minutes,
                accuracy_score,
                productivity_score,
                task_id,
                task["warehouse_id"],
                task["correlation_id"]
            )
        )

        # ---------------------------------
        # Event payload
        # ---------------------------------

        payload = {
            "event_type":
                EVENT_NAME,

            "task_id":
                task_id,

            "task_type":
                task["task_type"],

            "shipment_id":
                task["shipment_id"],

            "warehouse_id":
                task["warehouse_id"],

            "worker_id":
                worker_id,

            "status":
                "COMPLETED",

            "task_started_at":
                task["task_started_at"].isoformat()
                if task["task_started_at"]
                else None,

            "completed_at":
                completed_time.isoformat(),

            "actual_minutes":
                actual_minutes,

            "units_processed":
                task["quantity"],

            "accuracy_score":
                accuracy_score,

            "productivity_score":
                productivity_score,

            "correlation_id":
                str(
                    task["correlation_id"]
                )
        }

        # ---------------------------------
        # Outbox
        # ---------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="WAREHOUSE_TASK",
            aggregate_id=task_id,
            correlation_id=str(
                task["correlation_id"]
            ),
            payload=payload
        )

        # ---------------------------------
        # Log
        # ---------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "task_id":
                    task_id,

                "task_type":
                    task["task_type"],

                "shipment_id":
                    task["shipment_id"],

                "warehouse_id":
                    task["warehouse_id"],

                "worker_id":
                    worker_id,

                "actual_minutes":
                    actual_minutes,

                "completed_at":
                    completed_time,

                "productivity_score":
                    productivity_score,

                "correlation_id":
                    str(
                        task["correlation_id"]
                    )
            }
        )


if __name__ == "__main__":

    try:

        generate_task_completed()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
