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


EVENT_NAME = "GoodsReceived"


def _ensure_utc(value):
    """
    Normalize a datetime to timezone-aware UTC.

    PostgreSQL TIMESTAMPTZ values are returned as timezone-aware
    datetimes, while the simulation clock may return a naive
    datetime. Normalizing everything to UTC prevents comparison
    errors between the two forms.
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


def _get_goods_received_time(task):
    """
    GoodsReceived represents the completion of the receiving
    operation.

    It must always happen after ReceivingTaskStarted.

    Primary business anchor:
        task_started_at

    Fallbacks:
        assigned_at
        created_at
        simulation clock

    The actual receiving duration is then added to the
    selected business timestamp.
    """

    task_started_at = _ensure_utc(
        task.get(
            "task_started_at"
        )
    )

    assigned_at = _ensure_utc(
        task.get(
            "assigned_at"
        )
    )

    created_at = _ensure_utc(
        task.get(
            "created_at"
        )
    )

    simulation_now = _ensure_utc(
        get_simulation_now()
    )

    # ---------------------------------------------
    # Prefer the task's own business timeline.
    # ---------------------------------------------

    if task_started_at is not None:

        base_time = task_started_at

    elif assigned_at is not None:

        base_time = assigned_at

    elif created_at is not None:

        base_time = created_at

    else:

        base_time = simulation_now

    actual_minutes = random.randint(
        10,
        45
    )

    completed_at = (
        base_time +
        timedelta(
            minutes=actual_minutes
        )
    )

    return completed_at, actual_minutes


def generate_goods_received(
    task_id=None
):

    with Database() as db:

        # ------------------------------------
        # Find STARTED receiving task
        #
        # If task_id is supplied:
        #     process that exact task.
        #
        # Otherwise:
        #     use the oldest STARTED receiving task.
        # ------------------------------------

        if task_id:

            task = db.fetch_one(
                """
                SELECT
                    task_id,
                    shipment_id,
                    warehouse_id,
                    assigned_worker_id,
                    expected_quantity,
                    task_started_at,
                    assigned_at,
                    created_at,
                    correlation_id
                FROM warehouse_tasks
                WHERE task_id=%s
                  AND task_type='RECEIVING'
                  AND status='STARTED'
                LIMIT 1
                FOR UPDATE
                """,
                (
                    task_id,
                )
            )

        else:

            task = db.fetch_one(
                """
                SELECT
                    task_id,
                    shipment_id,
                    warehouse_id,
                    assigned_worker_id,
                    expected_quantity,
                    task_started_at,
                    assigned_at,
                    created_at,
                    correlation_id
                FROM warehouse_tasks
                WHERE task_type='RECEIVING'
                  AND status='STARTED'
                ORDER BY task_started_at
                LIMIT 1
                FOR UPDATE
                """
            )

        if not task:

            if task_id:

                raise Exception(
                    f"No STARTED receiving task found "
                    f"for task_id={task_id}"
                )

            raise Exception(
                "No STARTED receiving task found"
            )

        # ------------------------------------
        # Extract details
        # ------------------------------------

        task_id = task["task_id"]

        shipment_id = task["shipment_id"]

        warehouse_id = task["warehouse_id"]

        worker_id = task["assigned_worker_id"]

        expected_quantity = task[
            "expected_quantity"
        ]

        correlation_id = str(
            task["correlation_id"]
        )

        if not shipment_id:

            raise Exception(
                f"Receiving task {task_id} "
                "has no shipment_id"
            )

        if not warehouse_id:

            raise Exception(
                f"Receiving task {task_id} "
                "has no warehouse_id"
            )

        # ------------------------------------
        # Calculate business timestamp
        # ------------------------------------

        received_at, actual_minutes = (
            _get_goods_received_time(task)
        )

        # ------------------------------------
        # Get shipment items
        # ------------------------------------

        items = db.fetch_all(
            """
            SELECT
                shipment_item_id,
                product_id,
                shipped_quantity
            FROM shipment_items
            WHERE shipment_id=%s
            ORDER BY shipment_item_id
            """,
            (
                shipment_id,
            )
        )

        if not items:

            raise Exception(
                f"No shipment items found "
                f"for shipment {shipment_id}"
            )

        # ------------------------------------
        # Calculate received quantity
        # ------------------------------------

        total_received_quantity = sum(
            item["shipped_quantity"]
            for item in items
        )

        if total_received_quantity <= 0:

            raise Exception(
                f"Shipment {shipment_id} has "
                "no positive quantity to receive"
            )

        # ------------------------------------
        # Validate expected quantity
        # ------------------------------------

        if expected_quantity is not None:

            if (
                total_received_quantity
                != expected_quantity
            ):

                raise Exception(
                    f"""
Receiving quantity mismatch.

TASK:
{task_id}

EXPECTED:
{expected_quantity}

SHIPMENT ITEMS:
{total_received_quantity}
"""
                )

        # ------------------------------------
        # Process each shipment item
        # ------------------------------------

        for item in items:

            product_id = item["product_id"]

            shipped_quantity = (
                item["shipped_quantity"]
            )

            if shipped_quantity <= 0:

                raise Exception(
                    f"""
Invalid shipped quantity.

SHIPMENT:
{shipment_id}

PRODUCT:
{product_id}

QUANTITY:
{shipped_quantity}
"""
                )

            # --------------------------------
            # Update shipment item
            # --------------------------------

            db.execute(
                """
                UPDATE shipment_items
                SET
                    received_quantity=%s
                WHERE shipment_item_id=%s
                """,
                (
                    shipped_quantity,
                    item["shipment_item_id"]
                )
            )

            # --------------------------------
            # Find existing inventory
            # --------------------------------

            inventory = db.fetch_one(
                """
                SELECT
                    inventory_id,
                    on_hand_quantity,
                    reserved_quantity,
                    damaged_quantity,
                    available_quantity,
                    inventory_status
                FROM inventory
                WHERE product_id=%s
                  AND warehouse_id=%s
                FOR UPDATE
                """,
                (
                    product_id,
                    warehouse_id
                )
            )

            # --------------------------------
            # Create inventory if missing
            #
            # Newly received stock is not yet
            # AVAILABLE. It still needs putaway.
            # --------------------------------

            if not inventory:

                inventory = db.fetch_one(
                    """
                    INSERT INTO inventory
                    (
                        product_id,
                        warehouse_id,
                        on_hand_quantity,
                        reserved_quantity,
                        damaged_quantity,
                        last_updated_at,
                        inventory_status
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s,%s,%s
                    )
                    RETURNING
                        inventory_id
                    """,
                    (
                        product_id,
                        warehouse_id,
                        shipped_quantity,
                        0,
                        0,
                        received_at,
                        "RECEIVED"
                    )
                )

                inventory_id = inventory[
                    "inventory_id"
                ]

            else:

                inventory_id = inventory[
                    "inventory_id"
                ]

                db.execute(
                    """
                    UPDATE inventory
                    SET
                        on_hand_quantity =
                            on_hand_quantity + %s,

                        inventory_status='RECEIVED',

                        last_updated_at=%s

                    WHERE inventory_id=%s
                    """,
                    (
                        shipped_quantity,
                        received_at,
                        inventory_id
                    )
                )

            # --------------------------------
            # Create inventory transaction
            # --------------------------------

            db.execute(
                """
                INSERT INTO inventory_transactions
                (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    transaction_type,
                    quantity,
                    reference_type,
                    reference_id,
                    task_id,
                    shipment_id,
                    correlation_id
                )
                VALUES
                (
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s
                )
                """,
                (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    "STOCK_RECEIVED",
                    shipped_quantity,
                    "SHIPMENT",
                    shipment_id,
                    task_id,
                    shipment_id,
                    correlation_id
                )
            )

        # ------------------------------------
        # Complete receiving task
        # ------------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks
            SET
                status='COMPLETED',
                received_quantity=%s,
                actual_minutes=%s,
                completed_at=%s,
                task_completed_at=%s,
                completed_by=%s
            WHERE task_id=%s
            """,
            (
                total_received_quantity,
                actual_minutes,
                received_at,
                received_at,
                worker_id,
                task_id
            )
        )

        # ------------------------------------
        # Release receiving worker
        # ------------------------------------

        if worker_id:

            db.execute(
                """
                UPDATE workers
                SET
                    current_status='AVAILABLE'
                WHERE worker_id=%s
                """,
                (
                    worker_id,
                )
            )

        # ------------------------------------
        # Update inbound shipment
        # ------------------------------------

        db.execute(
            """
            UPDATE shipments
            SET
                shipment_status='RECEIVED',
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                received_at,
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
                received_at.isoformat(),

            "goodsReceived":
            {
                "taskId":
                    task_id,

                "shipmentId":
                    shipment_id,

                "warehouseId":
                    warehouse_id,

                "receivedQuantity":
                    total_received_quantity,

                "expectedQuantity":
                    expected_quantity,

                "workerId":
                    worker_id,

                "actualMinutes":
                    actual_minutes,

                "status":
                    "RECEIVED",

                "receivedAt":
                    received_at.isoformat()
            },

            "inventory":
            {
                "status":
                    "RECEIVED",

                "putawayRequired":
                    True
            },

            "correlationId":
                correlation_id
        }

        # ------------------------------------
        # Publish Outbox
        # ------------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="SHIPMENT",
            aggregate_id=shipment_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # ------------------------------------
        # Log
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

                "received_quantity":
                    total_received_quantity,

                "expected_quantity":
                    expected_quantity,

                "worker_id":
                    worker_id,

                "actual_minutes":
                    actual_minutes,

                "received_at":
                    received_at,

                "inventory_status":
                    "RECEIVED",

                "correlation_id":
                    correlation_id
            }
        )

        return {
            "task_id":
                task_id,

            "shipment_id":
                shipment_id,

            "warehouse_id":
                warehouse_id,

            "received_quantity":
                total_received_quantity,

            "received_at":
                received_at,

            "inventory_status":
                "RECEIVED"
        }


if __name__ == "__main__":

    try:

        import sys

        if len(sys.argv) > 1:

            generate_goods_received(
                sys.argv[1]
            )

        else:

            generate_goods_received()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
