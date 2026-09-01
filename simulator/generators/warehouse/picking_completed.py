from datetime import timedelta, timezone
import random
import sys


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "PickingCompleted"


def _ensure_utc(value):
    """
    Normalize a datetime to timezone-aware UTC.

    PostgreSQL TIMESTAMPTZ columns normally return timezone-aware
    datetime values, while the simulation clock may return a
    naive datetime.

    All timestamps are normalized before comparison so that
    Python never attempts to compare naive and aware datetimes.
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


def _get_picking_completed_time(
    task,
    actual_minutes
):
    """
    Calculate a causally valid PickingCompleted timestamp.

    PickingCompleted must always occur after PickingTaskStarted.

    Timestamp priority:

        1. task_started_at
        2. assigned_at
        3. created_at
        4. simulation clock

    The actual picking duration is added to the selected
    predecessor timestamp.
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
    # The task's own timeline is authoritative.
    # ---------------------------------------------

    if task_started_at is not None:

        base_time = task_started_at

    elif assigned_at is not None:

        base_time = assigned_at

    elif created_at is not None:

        base_time = created_at

    else:

        base_time = simulation_now

    completed_at = (
        base_time +
        timedelta(
            minutes=actual_minutes
        )
    )

    return completed_at


def generate_picking_completed(
    task_id
):

    with Database() as db:

        # =====================================================
        # 1. GET EXACT PICKING TASK
        # =====================================================

        task = db.fetch_one(
            """
            SELECT
                task_id,
                allocation_id,
                order_id,
                warehouse_id,
                product_id,
                location,
                quantity,
                assigned_worker_id,
                correlation_id,
                status,
                created_at,
                assigned_at,
                task_started_at
            FROM warehouse_tasks
            WHERE task_id=%s
              AND task_type='PICKING'
            FOR UPDATE
            """,
            (
                task_id,
            )
        )

        if not task:

            raise Exception(
                f"Picking task not found: {task_id}"
            )

        # =====================================================
        # 2. VALIDATE TASK STATUS
        # =====================================================

        if task["status"] != "STARTED":

            raise Exception(
                f"""
Picking task must be STARTED.

TASK:
{task_id}

CURRENT STATUS:
{task["status"]}
"""
            )

        # =====================================================
        # 3. EXTRACT TASK DETAILS
        # =====================================================

        allocation_id = task[
            "allocation_id"
        ]

        order_id = task[
            "order_id"
        ]

        warehouse_id = task[
            "warehouse_id"
        ]

        product_id = task[
            "product_id"
        ]

        quantity = task[
            "quantity"
        ]

        worker_id = task[
            "assigned_worker_id"
        ]

        correlation_id = str(
            task["correlation_id"]
        )

        # =====================================================
        # 4. VALIDATE TASK DETAILS
        # =====================================================

        if not allocation_id:

            raise Exception(
                f"""
Picking task has no allocation_id.

TASK:
{task_id}
"""
            )

        if not order_id:

            raise Exception(
                f"""
Picking task has no order_id.

TASK:
{task_id}
"""
            )

        if not product_id:

            raise Exception(
                f"""
Picking task has no product_id.

TASK:
{task_id}
"""
            )

        if quantity is None or quantity <= 0:

            raise Exception(
                f"""
Invalid picking quantity.

TASK:
{task_id}

QUANTITY:
{quantity}
"""
            )

        if not worker_id:

            raise Exception(
                f"""
Picking task has no assigned worker.

TASK:
{task_id}
"""
            )

        # =====================================================
        # 5. GET INVENTORY ALLOCATION
        # =====================================================

        allocation = db.fetch_one(
            """
            SELECT
                allocation_id,
                order_id,
                warehouse_id,
                product_id,
                inventory_id,
                location_id,
                allocated_quantity,
                allocation_status
            FROM inventory_allocations
            WHERE allocation_id=%s
            FOR UPDATE
            """,
            (
                allocation_id,
            )
        )

        if not allocation:

            raise Exception(
                f"""
Allocation not found.

ALLOCATION:
{allocation_id}
"""
            )

        # =====================================================
        # 6. VALIDATE ALLOCATION
        # =====================================================

        if allocation["order_id"] != order_id:

            raise Exception(
                "Allocation order mismatch"
            )

        if allocation["warehouse_id"] != warehouse_id:

            raise Exception(
                "Allocation warehouse mismatch"
            )

        if allocation["product_id"] != product_id:

            raise Exception(
                "Allocation product mismatch"
            )

        if allocation["allocated_quantity"] != quantity:

            raise Exception(
                f"""
Quantity mismatch.

ALLOCATION:
{allocation["allocated_quantity"]}

TASK:
{quantity}
"""
            )

        if allocation["allocation_status"] != "RESERVED":

            raise Exception(
                f"""
Allocation is not ready for picking.

ALLOCATION:
{allocation_id}

STATUS:
{allocation["allocation_status"]}
"""
            )

        # =====================================================
        # 7. GET INVENTORY
        # =====================================================

        inventory = db.fetch_one(
            """
            SELECT
                inventory_id,
                product_id,
                warehouse_id,
                location_id,
                on_hand_quantity,
                reserved_quantity,
                available_quantity,
                damaged_quantity,
                inventory_status
            FROM inventory
            WHERE inventory_id=%s
            FOR UPDATE
            """,
            (
                allocation["inventory_id"],
            )
        )

        if not inventory:

            raise Exception(
                "Inventory not found"
            )

        # =====================================================
        # 8. VALIDATE INVENTORY
        # =====================================================

        if inventory["product_id"] != product_id:

            raise Exception(
                "Inventory product mismatch"
            )

        if inventory["warehouse_id"] != warehouse_id:

            raise Exception(
                "Inventory warehouse mismatch"
            )

        if inventory["reserved_quantity"] < quantity:

            raise Exception(
                f"""
Reserved quantity insufficient.

RESERVED:
{inventory["reserved_quantity"]}

REQUIRED:
{quantity}
"""
            )

        if inventory["on_hand_quantity"] < quantity:

            raise Exception(
                f"""
On hand quantity insufficient.

ON HAND:
{inventory["on_hand_quantity"]}

REQUIRED:
{quantity}
"""
            )

        if inventory["inventory_status"] != "AVAILABLE":

            raise Exception(
                f"""
Inventory is not AVAILABLE for picking.

INVENTORY:
{inventory["inventory_id"]}

STATUS:
{inventory["inventory_status"]}
"""
            )

        if inventory["location_id"] is None:

            raise Exception(
                f"""
Inventory location is missing.

INVENTORY:
{inventory["inventory_id"]}
"""
            )

        # =====================================================
        # 9. GENERATE PICKING DURATION
        # =====================================================

        actual_minutes = random.randint(
            10,
            45
        )

        # =====================================================
        # 10. CALCULATE PICKING COMPLETION TIME
        # =====================================================

        completed_at = _get_picking_completed_time(
            task,
            actual_minutes
        )

        # =====================================================
        # 11. CALCULATE REMAINING INVENTORY
        # =====================================================

        remaining_quantity = (
            inventory["on_hand_quantity"]
            -
            quantity
        )

        remaining_reserved = (
            inventory["reserved_quantity"]
            -
            quantity
        )

        if remaining_quantity < 0:

            raise Exception(
                "Inventory cannot become negative"
            )

        if remaining_reserved < 0:

            raise Exception(
                "Reserved inventory cannot become negative"
            )

        # =====================================================
        # 12. DETERMINE INVENTORY STATUS
        #
        # Remaining stock:
        #
        #     > 0  -> AVAILABLE
        #     = 0  -> OUT_OF_STOCK
        #
        # RECEIVED is never used here.
        # RECEIVED is only for stock waiting for putaway.
        # =====================================================

        inventory_status = (
            "OUT_OF_STOCK"
            if remaining_quantity == 0
            else "AVAILABLE"
        )

        # =====================================================
        # 13. UPDATE INVENTORY
        # =====================================================

        db.execute(
            """
            UPDATE inventory
            SET
                on_hand_quantity =
                    on_hand_quantity - %s,

                reserved_quantity =
                    reserved_quantity - %s,

                inventory_status=%s,

                last_updated_at=%s
            WHERE inventory_id=%s
            """,
            (
                quantity,
                quantity,
                inventory_status,
                completed_at,
                inventory["inventory_id"]
            )
        )

        # =====================================================
        # 14. INVENTORY TRANSACTION
        # =====================================================

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
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s
            )
            """,
            (
                inventory["inventory_id"],
                product_id,
                warehouse_id,
                "PICKED",
                -quantity,
                "ORDER",
                order_id,
                task_id,
                correlation_id
            )
        )

        # =====================================================
        # 15. UPDATE ALLOCATION
        # =====================================================

        db.execute(
            """
            UPDATE inventory_allocations
            SET
                allocation_status='PICKED'
            WHERE allocation_id=%s
            """,
            (
                allocation_id,
            )
        )

        # =====================================================
        # 16. COMPLETE PICKING TASK
        # =====================================================

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
                completed_at,
                completed_at,
                worker_id,
                task_id
            )
        )

        # =====================================================
        # 17. CHECK WHETHER ALL ORDER ALLOCATIONS ARE PICKED
        # =====================================================

        remaining = db.fetch_one(
            """
            SELECT
                COUNT(*) AS count
            FROM inventory_allocations
            WHERE order_id=%s
              AND allocation_status!='PICKED'
            """,
            (
                order_id,
            )
        )

        all_items_picked = (
            remaining["count"] == 0
        )

        if all_items_picked:

            db.execute(
                """
                UPDATE orders
                SET
                    order_status='PICKED'
                WHERE order_id=%s
                """,
                (
                    order_id,
                )
            )

        # =====================================================
        # 18. RELEASE WORKER
        # =====================================================

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

        # =====================================================
        # 19. EVENT PAYLOAD
        # =====================================================

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                completed_at.isoformat(),

            "picking":
            {
                "taskId":
                    task_id,

                "allocationId":
                    allocation_id,

                "orderId":
                    order_id,

                "warehouseId":
                    warehouse_id,

                "productId":
                    product_id,

                "inventoryId":
                    inventory["inventory_id"],

                "locationId":
                    inventory["location_id"],

                "pickedQuantity":
                    quantity,

                "remainingInventory":
                    remaining_quantity,

                "remainingReserved":
                    remaining_reserved,

                "inventoryStatus":
                    inventory_status,

                "workerId":
                    worker_id,

                "actualMinutes":
                    actual_minutes,

                "status":
                    "COMPLETED",

                "startedAt":
                    task["task_started_at"].isoformat()
                    if task["task_started_at"]
                    else None,

                "completedAt":
                    completed_at.isoformat()
            },

            "orderPickingCompleted":
                all_items_picked,

            "correlationId":
                correlation_id
        }

        # =====================================================
        # 20. PUBLISH EVENT
        # =====================================================

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="WAREHOUSE_TASK",
            aggregate_id=task_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # =====================================================
        # 21. LOG
        # =====================================================

        log_event_success(
            EVENT_NAME,
            {
                "task_id":
                    task_id,

                "allocation_id":
                    allocation_id,

                "order_id":
                    order_id,

                "product_id":
                    product_id,

                "quantity":
                    quantity,

                "remaining_quantity":
                    remaining_quantity,

                "remaining_reserved":
                    remaining_reserved,

                "inventory_status":
                    inventory_status,

                "worker_id":
                    worker_id,

                "actual_minutes":
                    actual_minutes,

                "completed_at":
                    completed_at,

                "all_items_picked":
                    all_items_picked,

                "correlation_id":
                    correlation_id
            }
        )

        return {
            "task_id":
                task_id,

            "order_id":
                order_id,

            "picked_quantity":
                quantity,

            "remaining_inventory":
                remaining_quantity,

            "inventory_status":
                inventory_status,

            "all_items_picked":
                all_items_picked,

            "completed_at":
                completed_at
        }


if __name__ == "__main__":

    try:

        if len(sys.argv) < 2:

            raise Exception(
                """
Missing task id.

Usage:

python picking_completed.py TASK-000000001
"""
            )

        generate_picking_completed(
            sys.argv[1]
        )

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
