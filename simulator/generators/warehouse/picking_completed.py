from datetime import datetime, timezone
import sys

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "PickingCompleted"



def generate_picking_completed(task_id):

    with Database() as db:


        # =====================================================
        # 1. GET PICKING TASK
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
Picking task must be STARTED

TASK:
{task_id}

CURRENT STATUS:
{task["status"]}
"""
            )


        allocation_id = task["allocation_id"]

        order_id = task["order_id"]

        warehouse_id = task["warehouse_id"]

        product_id = task["product_id"]

        quantity = task["quantity"]

        worker_id = task["assigned_worker_id"]

        correlation_id = str(
            task["correlation_id"]
        )



        # =====================================================
        # 2. GET INVENTORY ALLOCATION
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
Allocation not found

{allocation_id}
"""
            )



        # =====================================================
        # 3. VALIDATIONS
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
Quantity mismatch

Allocation:
{allocation["allocated_quantity"]}

Task:
{quantity}
"""
            )


        if allocation["allocation_status"] != "RESERVED":

            raise Exception(
                f"""
Allocation not ready for picking

STATUS:
{allocation["allocation_status"]}
"""
            )



        # =====================================================
        # 4. GET INVENTORY
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



        if inventory["reserved_quantity"] < quantity:

            raise Exception(
                f"""
Reserved quantity insufficient

AVAILABLE RESERVED:
{inventory["reserved_quantity"]}

REQUIRED:
{quantity}
"""
            )


        if inventory["on_hand_quantity"] < quantity:

            raise Exception(
                f"""
On hand quantity insufficient

ON HAND:
{inventory["on_hand_quantity"]}

REQUIRED:
{quantity}
"""
            )



        now = datetime.now(
            timezone.utc
        )



        # =====================================================
        # 5. UPDATE INVENTORY AFTER PICK
        #
        # IMPORTANT:
        # Inventory status is not PICKED.
        # Remaining stock is still available.
        # Picking is recorded in inventory_transactions.
        # =====================================================


        remaining_quantity = (
            inventory["on_hand_quantity"]
            -
            quantity
        )


        if remaining_quantity < 0:

            raise Exception(
                "Inventory cannot become negative"
            )

        db.execute(
            """
            UPDATE inventory
            SET
                on_hand_quantity =
                    on_hand_quantity - %s,

                reserved_quantity =
                    reserved_quantity - %s,

                inventory_status =
                    CASE
                        WHEN on_hand_quantity - %s = 0
                            THEN 'OUT_OF_STOCK'
                        ELSE 'RECEIVED'
                    END,

                last_updated_at=%s

            WHERE inventory_id=%s
            """,
            (
                quantity,
                quantity,
                quantity,
                now,
                inventory["inventory_id"]
            )
        )


        # =====================================================
        # 6. INVENTORY TRANSACTION
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
        # 7. UPDATE ALLOCATION
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
        # 8. COMPLETE PICK TASK
        # =====================================================


        db.execute(
            """
            UPDATE warehouse_tasks

            SET

                status='COMPLETED',

                completed_at=%s,

                task_completed_at=%s,

                completed_by=%s


            WHERE task_id=%s

            """,
            (

                now,

                now,

                worker_id,

                task_id

            )
        )



        # =====================================================
        # 9. COMPLETE ORDER PICKING
        # =====================================================


        remaining = db.fetch_one(
            """
            SELECT COUNT(*) AS count

            FROM inventory_allocations

            WHERE order_id=%s

            AND allocation_status!='PICKED'

            """,
            (
                order_id,
            )
        )


        if remaining["count"] == 0:


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
        # 10. RELEASE WORKER
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
        # 11. EVENT
        # =====================================================


        payload = {


            "eventType":
                EVENT_NAME,


            "occurredAt":
                now.isoformat(),


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


                "pickedQuantity":
                    quantity,


                "remainingInventory":
                    remaining_quantity,


                "workerId":
                    worker_id,


                "status":
                    "COMPLETED"

            },


            "correlationId":
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



        # =====================================================
        # 12. LOG
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


                "worker_id":
                    worker_id,


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
                remaining_quantity

        }





if __name__ == "__main__":

    try:

        if len(sys.argv) < 2:

            raise Exception(
                """
Missing task id

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