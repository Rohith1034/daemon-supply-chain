from datetime import timedelta
import random
import uuid


from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "PackingTaskCreated"


def _get_packing_task_created_time(picking_task):
    """
    Calculate a causally valid PackingTaskCreated timestamp.

    PackingTaskCreated must happen after the related
    PickingCompleted event.

    The picking task's completion timestamp is the primary
    and authoritative business anchor.

    The simulation clock is used only as a fallback when
    the predecessor timestamp is unavailable.
    """

    picking_completed_at = picking_task.get(
        "task_completed_at"
    )

    if picking_completed_at is None:

        picking_completed_at = picking_task.get(
            "completed_at"
        )

    if picking_completed_at is None:

        picking_completed_at = picking_task.get(
            "task_started_at"
        )

    if picking_completed_at is None:

        picking_completed_at = picking_task.get(
            "created_at"
        )

    if picking_completed_at is None:

        picking_completed_at = get_simulation_now()

    # ---------------------------------------------
    # Packing task creation happens shortly after
    # picking has completed.
    # ---------------------------------------------

    return (
        picking_completed_at +
        timedelta(
            minutes=random.randint(
                5,
                30
            )
        )
    )


def generate_packing_task_created(
    picking_task_id=None
):

    with Database() as db:

        # =================================================
        # Find completed picking task
        #
        # When picking_task_id is supplied, always use
        # that exact picking task.
        #
        # Without it, preserve the existing behavior of
        # selecting the oldest completed picking task
        # that does not yet have a packing task.
        # =================================================

        if picking_task_id:

            picking_task = db.fetch_one(
                """
                SELECT
                    task_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    quantity,
                    location,
                    correlation_id,
                    task_completed_at,
                    completed_at,
                    task_started_at,
                    created_at
                FROM warehouse_tasks
                WHERE task_id=%s
                  AND task_type='PICKING'
                  AND status='COMPLETED'
                LIMIT 1
                FOR UPDATE
                """,
                (
                    picking_task_id,
                )
            )

        else:

            picking_task = db.fetch_one(
                """
                SELECT
                    task_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    quantity,
                    location,
                    correlation_id,
                    task_completed_at,
                    completed_at,
                    task_started_at,
                    created_at
                FROM warehouse_tasks
                WHERE task_type='PICKING'
                  AND status='COMPLETED'
                  AND NOT EXISTS
                  (
                      SELECT 1
                      FROM warehouse_tasks wt
                      WHERE wt.task_type='PACKING'
                        AND wt.picking_task_id =
                            warehouse_tasks.task_id
                  )
                ORDER BY task_completed_at
                LIMIT 1
                FOR UPDATE
                """
            )

        if not picking_task:

            if picking_task_id:

                raise Exception(
                    f"No completed picking task found "
                    f"for task_id={picking_task_id}"
                )

            raise Exception(
                "No completed picking task available for packing"
            )

        # -------------------------------------------------
        # Extract details
        # -------------------------------------------------

        picking_task_id = picking_task["task_id"]

        order_id = picking_task["order_id"]

        warehouse_id = picking_task["warehouse_id"]

        product_id = picking_task["product_id"]

        quantity = picking_task["quantity"]

        if not order_id:

            raise Exception(
                f"Picking task {picking_task_id} "
                "is missing order_id"
            )

        if quantity is None or quantity <= 0:

            raise Exception(
                f"Invalid picking quantity for task "
                f"{picking_task_id}: {quantity}"
            )

        # =================================================
        # Duplicate packing check
        # =================================================

        existing = db.fetch_one(
            """
            SELECT
                task_id,
                status
            FROM warehouse_tasks
            WHERE task_type='PACKING'
              AND picking_task_id=%s
              AND status IN
              (
                  'CREATED',
                  'STARTED',
                  'COMPLETED'
              )
            LIMIT 1
            """,
            (
                picking_task_id,
            )
        )

        if existing:

            raise Exception(
                f"""
Packing task already exists

Picking Task:
{picking_task_id}

Packing Task:
{existing['task_id']}

Status:
{existing['status']}
"""
            )

        # =================================================
        # Find available packer
        # =================================================

        worker = db.fetch_one(
            """
            SELECT
                worker_id
            FROM workers
            WHERE warehouse_id=%s
              AND current_status='AVAILABLE'
              AND employment_status='Active'
              AND LOWER(role) IN
              (
                  'packer',
                  'warehouse associate'
              )
            ORDER BY random()
            LIMIT 1
            FOR UPDATE
            """,
            (
                warehouse_id,
            )
        )

        if not worker:

            raise Exception(
                f"""
No packing worker available

Warehouse:
{warehouse_id}

Picking Task:
{picking_task_id}
"""
            )

        worker_id = worker["worker_id"]

        # =================================================
        # Create packing task
        # =================================================

        task_id = next_task_id(db)

        created_at = _get_packing_task_created_time(
            picking_task
        )

        correlation_id = (
            str(
                picking_task["correlation_id"]
            )
            if picking_task["correlation_id"]
            else str(uuid.uuid4())
        )

        estimated_minutes = random.randint(
            10,
            30
        )

        db.execute(
            """
            INSERT INTO warehouse_tasks
            (
                task_id,
                task_type,
                warehouse_id,
                order_id,
                product_id,
                picking_task_id,
                location,
                quantity,
                expected_quantity,
                priority,
                status,
                assigned_worker_id,
                estimated_minutes,
                created_at,
                assigned_at,
                created_by,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s
            )
            """,
            (
                task_id,
                "PACKING",
                warehouse_id,
                order_id,
                product_id,
                picking_task_id,
                picking_task["location"],
                quantity,
                quantity,
                "HIGH",
                "CREATED",
                worker_id,
                estimated_minutes,
                created_at,
                created_at,
                "WMS",
                correlation_id
            )
        )

        # =================================================
        # Reserve worker for packing task
        # =================================================

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

        # =================================================
        # Publish Event
        # =================================================

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                created_at.isoformat(),

            "packingTask":
            {
                "taskId":
                    task_id,

                "pickingTaskId":
                    picking_task_id,

                "orderId":
                    order_id,

                "warehouseId":
                    warehouse_id,

                "productId":
                    product_id,

                "quantity":
                    quantity,

                "workerId":
                    worker_id,

                "status":
                    "CREATED",

                "createdAt":
                    created_at.isoformat(),

                "pickingCompletedAt":
                    picking_task["task_completed_at"].isoformat()
                    if picking_task["task_completed_at"]
                    else (
                        picking_task["completed_at"].isoformat()
                        if picking_task["completed_at"]
                        else None
                    )
            },

            "correlationId":
                correlation_id
        }

        # =================================================
        # Outbox
        # =================================================

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="WAREHOUSE_TASK",
            aggregate_id=task_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # =================================================
        # Logging
        # =================================================

        log_event_success(
            EVENT_NAME,
            {
                "task_id":
                    task_id,

                "picking_task_id":
                    picking_task_id,

                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "created_at":
                    created_at,

                "correlation_id":
                    correlation_id
            }
        )

        return {
            "task_id":
                task_id,

            "picking_task_id":
                picking_task_id,

            "order_id":
                order_id
        }


if __name__ == "__main__":

    import sys

    try:

        if len(sys.argv) > 1:

            generate_packing_task_created(
                sys.argv[1]
            )

        else:

            generate_packing_task_created()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
