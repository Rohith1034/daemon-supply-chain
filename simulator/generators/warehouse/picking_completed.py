import random
from datetime import timedelta, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "PickingCompleted"



# ============================================================
# TIME HELPER
# ============================================================

def _ensure_utc(value):

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )



# ============================================================
# MAIN EVENT
# ============================================================

def generate_picking_completed(
        task_id=None,
        order_id=None
):


    with Database() as db:


        print(
f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

SEARCHING STARTED PICKING TASK

============================================================
"""
        )


        # ====================================================
        # FIND STARTED PICKING TASK
        # ====================================================

        if task_id:


            task = db.fetch_one(
                """
                SELECT
                    task_id,
                    warehouse_id,
                    order_id,
                    product_id,
                    quantity,
                    assigned_worker_id,
                    allocation_id,
                    correlation_id,
                    task_started_at

                FROM warehouse_tasks

                WHERE task_id=%s
                AND task_type='PICKING'
                AND status='STARTED'

                LIMIT 1
                """,
                (
                    task_id,
                )
            )


        elif order_id:


            task = db.fetch_one(
                """
                SELECT
                    task_id,
                    warehouse_id,
                    order_id,
                    product_id,
                    quantity,
                    assigned_worker_id,
                    allocation_id,
                    correlation_id,
                    task_started_at

                FROM warehouse_tasks

                WHERE order_id=%s
                AND task_type='PICKING'
                AND status='STARTED'

                ORDER BY task_started_at

                LIMIT 1
                """,
                (
                    order_id,
                )
            )


        else:


            task = db.fetch_one(
                """
                SELECT
                    task_id,
                    warehouse_id,
                    order_id,
                    product_id,
                    quantity,
                    assigned_worker_id,
                    allocation_id,
                    correlation_id,
                    task_started_at

                FROM warehouse_tasks

                WHERE task_type='PICKING'
                AND status='STARTED'

                ORDER BY task_started_at

                LIMIT 1
                """
            )



        if not task:

            raise Exception(
                "No STARTED PICKING task found"
            )



        task_id = task["task_id"]

        order_id = task["order_id"]

        worker_id = task["assigned_worker_id"]

        warehouse_id = task["warehouse_id"]

        product_id = task["product_id"]

        quantity = task["quantity"]

        allocation_id = task["allocation_id"]

        correlation_id = str(
            task["correlation_id"]
        )



        print(
f"""
============================================================
PICKING TASK FOUND

TASK:
{task_id}

ORDER:
{order_id}

PRODUCT:
{product_id}

QUANTITY:
{quantity}

WORKER:
{worker_id}

============================================================
"""
        )



        # ====================================================
        # FIND INVENTORY ALLOCATION
        # ====================================================

        allocation = db.fetch_one(
            """
            SELECT
                inventory_id,
                product_id,
                location_id,
                allocation_status

            FROM inventory_allocations

            WHERE allocation_id=%s

            LIMIT 1
            """,
            (
                allocation_id,
            )
        )


        if not allocation:

            raise Exception(
                f"No inventory allocation found {allocation_id}"
            )



        inventory_id = allocation["inventory_id"]



        # ====================================================
        # COMPLETION TIME
        # ====================================================

        completed_at = (
            _ensure_utc(
                get_simulation_now()
            )
            +
            timedelta(
                minutes=random.randint(5,30)
            )
        )



        # ====================================================
        # LOCK INVENTORY
        # ====================================================

        inventory = db.fetch_one(
            """
            SELECT
                on_hand_quantity,
                reserved_quantity,
                available_quantity

            FROM inventory

            WHERE inventory_id=%s

            FOR UPDATE
            """,
            (
                inventory_id,
            )
        )



        if not inventory:

            raise Exception(
                f"Inventory not found {inventory_id}"
            )



        # ====================================================
        # INVENTORY MOVEMENT
        #
        # RESERVED -> PICKED
        #
        # Before:
        # on_hand = 100
        # reserved = 20
        # available = 80
        #
        # After:
        # on_hand = 80
        # reserved = 0
        # available = 80
        #
        # available_quantity automatically calculated
        #
        # ====================================================


        new_on_hand = (
            inventory["on_hand_quantity"]
            -
            quantity
        )


        new_reserved = (
            inventory["reserved_quantity"]
            -
            quantity
        )



        if new_on_hand < 0:

            raise Exception(
                "Insufficient on hand inventory"
            )


        if new_reserved < 0:

            raise Exception(
                "Reserved inventory mismatch"
            )



        db.execute(
            """
            UPDATE inventory

            SET
                on_hand_quantity=%s,
                reserved_quantity=%s,
                last_updated_at=%s

            WHERE inventory_id=%s

            """,
            (
                new_on_hand,
                new_reserved,
                completed_at,
                inventory_id
            )
        )



        # ====================================================
        # INVENTORY TRANSACTION
        # ====================================================

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
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)

            """,
            (
                inventory_id,
                product_id,
                warehouse_id,
                "STOCK_PICKED",
                quantity,
                "PICKING_TASK",
                task_id,
                task_id,
                correlation_id
            )
        )



        # ====================================================
        # UPDATE ALLOCATION
        # ====================================================

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



        # ====================================================
        # COMPLETE TASK
        # ====================================================

        actual_minutes = random.randint(
            5,
            30
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

            """,
            (
                actual_minutes,
                completed_at,
                completed_at,
                worker_id,
                task_id
            )
        )



        # ====================================================
        # RELEASE WORKER
        # ====================================================

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



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================

        payload = {

            "event_type":
                EVENT_NAME,


            "occurred_at":
                completed_at.isoformat(),


            "picking":
            {

                "task_id":
                    task_id,

                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "product_id":
                    product_id,

                "quantity":
                    quantity,

                "inventory_id":
                    inventory_id,

                "status":
                    "COMPLETED"

            },


            "correlation_id":
                correlation_id

        }



        # ====================================================
        # OUTBOX
        # ====================================================

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="TASK",
            aggregate_id=task_id,
            correlation_id=correlation_id,
            payload=payload
        )



        # ====================================================
        # LOG
        # ====================================================

        log_event_success(
            EVENT_NAME,
            {
                "task_id": task_id,
                "order_id": order_id,
                "quantity": quantity,
                "worker_id": worker_id
            }
        )



        print(
f"""
============================================================
EVENT SUCCESS

EVENT:
{EVENT_NAME}

TASK:
{task_id}

ORDER:
{order_id}

STATUS:
COMPLETED

============================================================
"""
        )



        return {

            "task_id":
                task_id,

            "order_id":
                order_id,

            "status":
                "COMPLETED"

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_picking_completed()


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise