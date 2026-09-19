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


EVENT_NAME = "PickingTaskCreated"



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



def _generate_task_id():

    suffix = ''.join(
        random.choices(
            string.ascii_uppercase +
            string.digits,
            k=6
        )
    )

    return f"PICK-{suffix}"



# ============================================================
# MAIN EVENT
# ============================================================

def generate_picking_task_created(
        order_id=None
):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CREATING PICKING TASKS

============================================================
"""
        )


        # ====================================================
        # FIND RESERVED ALLOCATIONS
        # ====================================================


        if order_id:


            allocations = db.fetch_all(
                """
                SELECT
                    ia.allocation_id,
                    ia.order_id,
                    ia.warehouse_id,
                    ia.product_id,
                    ia.allocated_quantity,
                    ia.inventory_id,
                    ia.location_id,
                    ia.correlation_id
                FROM inventory_allocations ia
                WHERE ia.order_id=%s
                AND ia.allocation_status='RESERVED'
                AND NOT EXISTS
                (
                    SELECT 1
                    FROM warehouse_tasks wt
                    WHERE wt.allocation_id = ia.allocation_id
                    AND wt.task_type='PICKING'
                )
                ORDER BY ia.allocation_id
                """,
                (
                    order_id,
                )
            )


        else:


            allocations = db.fetch_all(
                """
                SELECT
                    allocation_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    allocated_quantity,
                    inventory_id,
                    location_id,
                    correlation_id
                FROM inventory_allocations
                WHERE allocation_status='RESERVED'
                ORDER BY allocated_at
                """
            )



        if not allocations:

            raise Exception(
                "No RESERVED allocations found"
            )



        order_id = allocations[0]["order_id"]

        warehouse_id = allocations[0]["warehouse_id"]

        correlation_id = str(
            allocations[0]["correlation_id"]
        )



        created_at = _ensure_utc(
            get_simulation_now()
        )



        tasks = []



        # ====================================================
        # CREATE PICKING TASK PER ALLOCATION
        # ====================================================


        for allocation in allocations:


            task_id = _generate_task_id()



            estimated_minutes = random.randint(
                5,
                20
            )



            db.execute(
                """
                INSERT INTO warehouse_tasks
                (
                    task_id,
                    task_type,
                    warehouse_id,
                    product_id,
                    location,
                    quantity,
                    priority,
                    status,
                    created_at,
                    order_id,
                    allocation_id,
                    correlation_id,
                    expected_quantity,
                    estimated_minutes
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    task_id,
                    "PICKING",
                    warehouse_id,
                    allocation["product_id"],
                    allocation["location_id"],
                    allocation["allocated_quantity"],
                    "NORMAL",
                    "CREATED",
                    created_at,
                    order_id,
                    allocation["allocation_id"],
                    correlation_id,
                    allocation["allocated_quantity"],
                    estimated_minutes
                )
            )



            tasks.append(
                {

                    "task_id":
                        task_id,

                    "allocation_id":
                        allocation["allocation_id"],

                    "product_id":
                        allocation["product_id"],

                    "location_id":
                        allocation["location_id"],

                    "quantity":
                        allocation["allocated_quantity"],

                    "status":
                        "CREATED"

                }
            )



        # ====================================================
        # UPDATE ORDER STATUS
        # ====================================================






        # ====================================================
        # EVENT PAYLOAD
        # ====================================================


        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                created_at.isoformat(),



            "picking":

            {

                "order_id":
                    order_id,


                "warehouse_id":
                    warehouse_id,


                "tasks":
                    tasks,


                "task_count":
                    len(tasks)

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



        # ====================================================
        # LOG
        # ====================================================


        log_event_success(
            EVENT_NAME,
            {

                "order_id":
                    order_id,

                "tasks_created":
                    len(tasks)

            }
        )



        print(
            f"""
============================================================
EVENT SUCCESS

EVENT:
{EVENT_NAME}

ORDER:
{order_id}

TASKS CREATED:
{len(tasks)}

============================================================
"""
        )


        return {

            "order_id":
                order_id,

            "tasks":
                tasks

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_picking_task_created()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise