import random
import string
from datetime import timedelta, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "PackingCompleted"



# ============================================================
# HELPERS
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



def _generate_package_id():

    suffix = ''.join(
        random.choices(
            string.ascii_uppercase +
            string.digits,
            k=6
        )
    )

    return f"PKG-{suffix}"



# ============================================================
# MAIN EVENT
# ============================================================

def generate_packing_completed(task_id=None):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

COMPLETING PACKING
============================================================
"""
        )


        # ====================================================
        # FIND PACKING TASK
        # ====================================================

        if task_id:

            task = db.fetch_one(
                """
                SELECT
                    task_id,
                    order_id,
                    warehouse_id,
                    quantity,
                    correlation_id
                FROM warehouse_tasks
                WHERE task_id=%s
                  AND task_type='PACKING'
                  AND status='CREATED'
                LIMIT 1
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
                    order_id,
                    warehouse_id,
                    quantity,
                    correlation_id
                FROM warehouse_tasks
                WHERE task_type='PACKING'
                  AND status='CREATED'
                ORDER BY created_at
                LIMIT 1
                """
            )


        if not task:

            raise Exception(
                "No CREATED PACKING task found"
            )


        task_id = task["task_id"]
        order_id = task["order_id"]
        warehouse_id = task["warehouse_id"]
        quantity = task["quantity"]

        correlation_id = str(
            task["correlation_id"]
        )



        # ====================================================
        # FIND PACKING WORKER
        # ====================================================

        worker = db.fetch_one(
            """
            SELECT
                worker_id
            FROM workers
            WHERE warehouse_id=%s
              AND current_status='AVAILABLE'
            ORDER BY worker_id
            LIMIT 1
            """,
            (
                warehouse_id,
            )
        )


        if not worker:

            raise Exception(
                f"No packing worker available "
                f"in warehouse {warehouse_id}"
            )


        worker_id = worker["worker_id"]



        start_time = _ensure_utc(
            get_simulation_now()
        )


        duration = random.randint(
            5,
            20
        )


        completed_time = (
            start_time +
            timedelta(
                minutes=duration
            )
        )



        # ====================================================
        # CREATE PACKAGE
        # ====================================================

        package_id = _generate_package_id()



        db.execute(
            """
            INSERT INTO packages
            (
                package_id,
                order_id,
                warehouse_id,
                package_type,
                total_items,
                total_quantity,
                weight_kg,
                package_status,
                packed_by,
                packed_at,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                package_id,
                order_id,
                warehouse_id,
                "STANDARD",
                1,
                quantity,
                round(
                    random.uniform(
                        0.5,
                        10
                    ),
                    3
                ),
                "PACKED",
                worker_id,
                completed_time,
                correlation_id
            )
        )



        # ====================================================
        # CREATE PACKAGE ITEMS
        # ====================================================

        picking_items = db.fetch_all(
            """
            SELECT
                product_id,
                quantity
            FROM warehouse_tasks
            WHERE order_id=%s
              AND task_type='PICKING'
              AND status='COMPLETED'
            """,
            (
                order_id,
            )
        )


        for item in picking_items:

            db.execute(
                """
                INSERT INTO package_items
                (
                    package_id,
                    product_id,
                    quantity
                )
                VALUES
                (
                    %s,%s,%s
                )
                """,
                (
                    package_id,
                    item["product_id"],
                    item["quantity"]
                )
            )



        # ====================================================
        # UPDATE PACKING TASK
        # ====================================================

        db.execute(
            """
            UPDATE warehouse_tasks
            SET
                status='COMPLETED',
                assigned_worker_id=%s,
                task_started_at=%s,
                task_completed_at=%s,
                completed_at=%s,
                completed_by=%s,
                actual_minutes=%s
            WHERE task_id=%s
            """,
            (
                worker_id,
                start_time,
                completed_time,
                completed_time,
                worker_id,
                duration,
                task_id
            )
        )



        # ====================================================
        # UPDATE ORDER
        # ====================================================

        db.execute(
            """
            UPDATE orders
            SET
                order_status='PACKED'
            WHERE order_id=%s
            """,
            (
                order_id,
            )
        )



        # ====================================================
        # RELEASE WORKER
        # ====================================================

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
                completed_time.isoformat(),


            "packing":
            {
                "task_id":
                    task_id,

                "package_id":
                    package_id,

                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "quantity":
                    quantity,

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
            aggregate_type="ORDER",
            aggregate_id=order_id,
            correlation_id=correlation_id,
            payload=payload
        )



        log_event_success(
            EVENT_NAME,
            {
                "task_id": task_id,
                "package_id": package_id,
                "order_id": order_id,
                "quantity": quantity
            }
        )


        return {

            "task_id": task_id,

            "package_id": package_id,

            "status":
                "COMPLETED"

        }



# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_packing_completed()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise