from datetime import timedelta
import random

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


EVENT_NAME = "ReceivingTaskCreated"


def _get_receiving_task_time(shipment):
    """
    Receiving task creation must happen after the shipment
    has actually been delivered.

    The shipment's actual_delivery is the primary business
    anchor. The simulation clock is used only when the
    shipment delivery timestamp is unavailable.

    A small warehouse processing delay is added so that
    shipment delivery and task creation do not always
    occur at exactly the same timestamp.
    """

    simulation_now = get_simulation_now()

    delivery_time = shipment.get(
        "actual_delivery"
    )

    shipment_updated_at = shipment.get(
        "updated_at"
    )

    shipment_date = shipment.get(
        "shipment_date"
    )

    candidates = [
        candidate
        for candidate in [
            delivery_time,
            shipment_updated_at,
            shipment_date,
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
            minutes=random.randint(
                10,
                120
            )
        )
    )


def generate_receiving_task_created():

    with Database() as db:

        # ------------------------------------
        # Find delivered shipment
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id,
                total_quantity,
                shipment_date,
                actual_delivery,
                updated_at
            FROM shipments
            WHERE shipment_status='DELIVERED'
              AND receiving_task_created=false
            ORDER BY updated_at
            LIMIT 1
            """
        )

        if not shipment:
            raise Exception(
                "No DELIVERED shipment found"
            )

        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )

        quantity = shipment["total_quantity"]

        # ------------------------------------
        # Business timestamp
        #
        # This must occur after the shipment
        # has been delivered.
        # ------------------------------------

        created_at = _get_receiving_task_time(
            shipment
        )

        # ------------------------------------
        # Pick receiving worker
        # ------------------------------------

        worker = db.fetch_one(
            """
            SELECT
                worker_id
            FROM workers
            WHERE warehouse_id=%s
              AND current_status='AVAILABLE'
            ORDER BY random()
            LIMIT 1
            """,
            (
                warehouse_id,
            )
        )

        if not worker:
            raise Exception(
                "No available warehouse worker found for receiving"
            )

        worker_id = worker["worker_id"]

        # ------------------------------------
        # Generate task id
        # ------------------------------------

        task_id = next_task_id(db)

        dock = "DOCK-001"

        # ------------------------------------
        # Insert warehouse task
        # ------------------------------------

        db.execute(
            """
            INSERT INTO warehouse_tasks
            (
                task_id,
                task_type,
                warehouse_id,
                shipment_id,
                quantity,
                priority,
                status,
                assigned_worker_id,
                estimated_minutes,
                created_at,
                assigned_at,
                created_by,
                correlation_id,
                dock_location,
                expected_quantity
            )
            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s
            )
            """,
            (
                task_id,
                "RECEIVING",
                warehouse_id,
                shipment_id,
                quantity,
                "HIGH",
                "CREATED",
                worker_id,
                30,
                created_at,
                created_at,
                "SYSTEM",
                correlation_id,
                dock,
                quantity
            )
        )

        # ------------------------------------
        # Mark receiving task as created
        # ------------------------------------

        db.execute(
            """
            UPDATE shipments
            SET
                receiving_task_created=true,
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                created_at,
                shipment_id
            )
        )

        # ------------------------------------
        # Event payload
        # ------------------------------------

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                created_at.isoformat(),

            "receivingTask":
            {
                "taskId":
                    task_id,

                "shipmentId":
                    shipment_id,

                "warehouseId":
                    warehouse_id,

                "workerId":
                    worker_id,

                "dockLocation":
                    dock,

                "expectedQuantity":
                    quantity,

                "status":
                    "CREATED"
            },

            "correlationId":
                correlation_id
        }

        # ------------------------------------
        # Publish Outbox Event
        # ------------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="WAREHOUSE_TASK",
            aggregate_id=task_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # ------------------------------------
        # Logging
        # ------------------------------------

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

                "expected_quantity":
                    quantity,

                "dock_location":
                    dock,

                "created_at":
                    created_at,

                "correlation_id":
                    correlation_id
            }
        )


if __name__ == "__main__":

    try:

        generate_receiving_task_created()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
