from datetime import datetime, timezone
import random
import sys

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "PickingTaskCompleted"


def generate_picking_completed(task_id):

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
                status
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

        if task["status"] != "STARTED":

            raise Exception(
                f"""
Picking task is not STARTED.

TASK:
{task_id}

STATUS:
{task["status"]}
"""
            )

        allocation_id = task["allocation_id"]

        order_id = task["order_id"]

        warehouse_id = task["warehouse_id"]

        product_id = task["product_id"]

        location_id = task["location"]

        quantity = task["quantity"]

        worker_id = task["assigned_worker_id"]

        correlation_id = str(
            task["correlation_id"]
        )

        if not allocation_id:

            raise Exception(
                f"""
Picking task has no allocation_id.

TASK:
{task_id}
"""
            )

        # =====================================================
        # 2. GET EXACT ALLOCATION
        # =====================================================

        allocation = db.fetch_one(
            """
            SELECT
                allocation_id,
                order_id,
                warehouse_id,
                product_id,
                allocated_quantity,
                allocation_status,
                inventory_id,
                location_id,
                correlation_id
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
Inventory allocation not found.

ALLOCATION:
{allocation_id}

TASK:
{task_id}
"""
            )

        # -----------------------------------------------------
        # Relationship validation
        # -----------------------------------------------------

        if allocation["order_id"] != order_id:

            raise Exception(
                "Allocation/order relationship mismatch"
            )

        if allocation["product_id"] != product_id:

            raise Exception(
                "Allocation/product relationship mismatch"
            )

        if allocation["warehouse_id"] != warehouse_id:

            raise Exception(
                "Allocation/warehouse relationship mismatch"
            )

        if allocation["allocation_status"] != "RESERVED":

            raise Exception(
                f"""
Allocation is not RESERVED.

ALLOCATION:
{allocation_id}

STATUS:
{allocation["allocation_status"]}
"""
            )

        if allocation["allocated_quantity"] != quantity:

            raise Exception(
                f"""
Allocation/task quantity mismatch.

ALLOCATION:
{allocation["allocated_quantity"]}

TASK:
{quantity}
"""
            )

        # =====================================================
        # 3. GET EXACT INVENTORY
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
                damaged_quantity
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
                f"""
Inventory not found.

INVENTORY:
{allocation["inventory_id"]}
"""
            )

        if inventory["reserved_quantity"] < quantity:

            raise Exception(
                f"""
Reserved quantity less than picking quantity.

INVENTORY:
{inventory["inventory_id"]}

PRODUCT:
{product_id}

RESERVED:
{inventory["reserved_quantity"]}

REQUIRED:
{quantity}
"""
            )

        if inventory["on_hand_quantity"] < quantity:

            raise Exception(
                f"""
On-hand quantity less than picking quantity.

ON HAND:
{inventory["on_hand_quantity"]}

REQUIRED:
{quantity}
"""
            )

        # =====================================================
        # 4. EXECUTE PICK
        # =====================================================

        now = datetime.now(
            timezone.utc
        )

        db.execute(
            """
            UPDATE inventory
            SET
                on_hand_quantity =
                    on_hand_quantity - %s,

                reserved_quantity =
                    reserved_quantity - %s,

                inventory_status='PICKED',

                last_updated_at=%s

            WHERE inventory_id=%s
            """,
            (
                quantity,
                quantity,
                now,
                inventory["inventory_id"]
            )
        )

        # =====================================================
        # 5. INVENTORY TRANSACTION
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
                %s,%s,%s,%s,%s,%s,%s,%s,%s
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
        # 6. UPDATE ALLOCATION
        # =====================================================

        db.execute(
            """
            UPDATE inventory_allocations
            SET
                allocation_status='PICKED'
            WHERE allocation_id=%s
              AND allocation_status='RESERVED'
            """,
            (
                allocation_id,
            )
        )

        # =====================================================
        # 7. COMPLETE TASK
        # =====================================================

        actual_minutes = random.randint(
            3,
            15
        )

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
              AND status='STARTED'
            """,
            (
                actual_minutes,
                now,
                now,
                worker_id,
                task_id
            )
        )

        # =====================================================
        # 8. RELEASE WORKER
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
        # 9. EVENT
        # =====================================================

        payload = {
            "eventType": EVENT_NAME,

            "occurredAt": now.isoformat(),

            "picking": {
                "taskId": task_id,

                "allocationId": allocation_id,

                "orderId": order_id,

                "warehouseId": warehouse_id,

                "productId": product_id,

                "inventoryId": inventory["inventory_id"],

                "locationId": location_id,

                "pickedQuantity": quantity,

                "workerId": worker_id,

                "status": "COMPLETED"
            },

            "correlationId": correlation_id
        }

        publish_event(
            db=db,

            event_type=EVENT_NAME,

            aggregate_type="WAREHOUSE_TASK",

            aggregate_id=task_id,

            correlation_id=correlation_id,

            payload=payload
        )

        # =====================================================
        # 10. LOG
        # =====================================================

        log_event_success(
            EVENT_NAME,
            {
                "task_id": task_id,

                "allocation_id": allocation_id,

                "order_id": order_id,

                "product_id": product_id,

                "warehouse_id": warehouse_id,

                "inventory_id": inventory["inventory_id"],

                "quantity": quantity,

                "worker_id": worker_id,

                "correlation_id": correlation_id
            }
        )

        return {
            "task_id": task_id,

            "allocation_id": allocation_id,

            "order_id": order_id
        }


# =========================================================
# SCRIPT ENTRY POINT
# =========================================================

if __name__ == "__main__":

    try:

        if len(sys.argv) < 2:

            raise Exception(
                """
Missing picking task ID.

Usage:

python picking_completed.py TASK-000000001
"""
            )

        task_id = sys.argv[1]

        generate_picking_completed(
            task_id
        )

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise