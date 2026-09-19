import random
import string
from datetime import timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "PackingTaskCreated"


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
# ID GENERATOR
# ============================================================

def _generate_task_id():

    suffix = ''.join(
        random.choices(
            string.ascii_uppercase +
            string.digits,
            k=6
        )
    )

    return f"PACK-{suffix}"


# ============================================================
# MAIN EVENT
# ============================================================

def generate_packing_task_created(order_id=None):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CREATING PACKING TASK

============================================================
"""
        )


        # ====================================================
        # FIND COMPLETED PICKING TASKS
        # ====================================================


        if order_id:


            picking_tasks = db.fetch_all(
                """
                SELECT
                    task_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    quantity,
                    correlation_id
                FROM warehouse_tasks
                WHERE order_id=%s
                  AND task_type='PICKING'
                  AND status='COMPLETED'
                ORDER BY task_id
                """,
                (
                    order_id,
                )
            )


        else:


            picking_tasks = db.fetch_all(
                """
                SELECT
                    task_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    quantity,
                    correlation_id
                FROM warehouse_tasks
                WHERE task_type='PICKING'
                  AND status='COMPLETED'
                ORDER BY task_completed_at
                """
            )


        if not picking_tasks:

            raise Exception(
                "No COMPLETED PICKING tasks found"
            )


        order_id = picking_tasks[0]["order_id"]

        warehouse_id = picking_tasks[0]["warehouse_id"]

        correlation_id = str(
            picking_tasks[0]["correlation_id"]
        )



        # ====================================================
        # VERIFY ALL PICKING COMPLETED
        # ====================================================


        pending_tasks = db.fetch_one(
            """
            SELECT
                COUNT(*) AS count
            FROM warehouse_tasks
            WHERE order_id=%s
              AND task_type='PICKING'
              AND status!='COMPLETED'
            """,
            (
                order_id,
            )
        )


        if pending_tasks["count"] > 0:


            raise Exception(
                f"""
Picking not completed completely

ORDER:
{order_id}

Remaining tasks:
{pending_tasks["count"]}
"""
            )



        # ====================================================
        # DUPLICATE PACKING CHECK
        # ====================================================


        existing = db.fetch_one(
            """
            SELECT
                task_id,
                status
            FROM warehouse_tasks
            WHERE order_id=%s
              AND task_type='PACKING'
            LIMIT 1
            """,
            (
                order_id,
            )
        )


        if existing:


            raise Exception(
                f"""
Packing task already exists

TASK:
{existing["task_id"]}

STATUS:
{existing["status"]}
"""
            )



        # ====================================================
        # CALCULATE TOTAL QUANTITY
        # ====================================================


        total_quantity = sum(
            task["quantity"]
            for task in picking_tasks
        )



        created_at = _ensure_utc(
            get_simulation_now()
        )



        estimated_minutes = random.randint(
            5,
            20
        )


        task_id = _generate_task_id()



        # ====================================================
        # CREATE PACKING TASK
        # ====================================================


        db.execute(
            """
            INSERT INTO warehouse_tasks
            (
                task_id,
                task_type,
                warehouse_id,
                order_id,
                quantity,
                priority,
                status,
                expected_quantity,
                estimated_minutes,
                created_at,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s
            )
            """,
            (
                task_id,
                "PACKING",
                warehouse_id,
                order_id,
                total_quantity,
                "NORMAL",
                "CREATED",
                total_quantity,
                estimated_minutes,
                created_at,
                correlation_id
            )
        )



        print(
            f"""
============================================================
PACKING TASK CREATED

TASK:
{task_id}

ORDER:
{order_id}

QUANTITY:
{total_quantity}

============================================================
"""
        )



        # ====================================================
        # UPDATE ORDER STATUS
        # ====================================================


        db.execute(
            """
            UPDATE orders
            SET
                order_status='PACKING'
            WHERE order_id=%s
            """,
            (
                order_id,
            )
        )



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================


        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                created_at.isoformat(),


            "packing_task":
            {

                "task_id":
                    task_id,

                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "quantity":
                    total_quantity,

                "status":
                    "CREATED"

            },


            "correlation_id":
                correlation_id

        }



        # ====================================================
        # OUTBOX EVENT
        # ====================================================


        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="ORDER",
            aggregate_id=order_id,
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
                "quantity": total_quantity
            }
        )


        return {

            "task_id":
                task_id,

            "order_id":
                order_id,

            "status":
                "CREATED"

        }



# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_packing_task_created()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise