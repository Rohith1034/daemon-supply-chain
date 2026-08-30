from datetime import datetime, timezone
import random
import sys

from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "PickingTaskCreated"



def generate_picking_task_created(reference_id):


    with Database() as db:


        # =====================================================
        # 1. FIND ORDER FROM ALLOCATION OR ORDER ID
        # =====================================================

        order = db.fetch_one(
            """

            SELECT
                order_id

            FROM inventory_allocations

            WHERE allocation_id=%s


            """,
            (
                reference_id,
            )
        )


        if order:

            order_id = order["order_id"]

        else:

            # maybe direct order id was passed

            order_check = db.fetch_one(
                """

                SELECT
                    order_id

                FROM orders

                WHERE order_id=%s

                """,
                (
                    reference_id,
                )
            )


            if not order_check:

                raise Exception(
                    f"""
Order or Allocation not found:

{reference_id}
"""
                )


            order_id = reference_id



        # =====================================================
        # 2. FETCH ALL RESERVED ALLOCATIONS
        # =====================================================

        allocations = db.fetch_all(
            """

            SELECT

                allocation_id,
                order_id,
                product_id,
                warehouse_id,
                allocated_quantity,
                location_id,
                correlation_id


            FROM inventory_allocations


            WHERE order_id=%s

            AND allocation_status='RESERVED'


            ORDER BY allocation_id


            """,
            (
                order_id,
            )
        )


        if not allocations:

            raise Exception(
                f"""
No RESERVED allocations found

ORDER:
{order_id}
"""
            )



        created_tasks=[]



        # =====================================================
        # 3. CREATE TASK PER ALLOCATION
        # =====================================================

        for allocation in allocations:


            allocation_id = allocation["allocation_id"]

            product_id = allocation["product_id"]

            warehouse_id = allocation["warehouse_id"]

            quantity = allocation["allocated_quantity"]

            correlation_id = str(
                allocation["correlation_id"]
            )


            # -----------------------------------------
            # Skip existing task
            # -----------------------------------------

            existing = db.fetch_one(
                """

                SELECT task_id

                FROM warehouse_tasks

                WHERE allocation_id=%s

                AND task_type='PICKING'

                """,
                (
                    allocation_id,
                )
            )


            if existing:

                continue



            # -----------------------------------------
            # Find available worker
            # -----------------------------------------

            worker = db.fetch_one(
                """

                SELECT worker_id

                FROM workers

                WHERE warehouse_id=%s

                AND current_status='AVAILABLE'

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
No available picker

WAREHOUSE:
{warehouse_id}
"""
                )


            worker_id = worker["worker_id"]



            now = datetime.now(
                timezone.utc
            )



            task_id = next_task_id(db)



            # -----------------------------------------
            # INSERT TASK
            # -----------------------------------------

            db.execute(
                """

                INSERT INTO warehouse_tasks

                (

                    task_id,

                    task_type,

                    warehouse_id,

                    order_id,

                    product_id,

                    quantity,

                    priority,

                    status,

                    assigned_worker_id,

                    estimated_minutes,

                    created_at,

                    assigned_at,

                    created_by,

                    correlation_id,

                    allocation_id,

                    expected_quantity

                )


                VALUES

                (

                    %s,%s,%s,%s,%s,

                    %s,%s,%s,%s,%s,

                    %s,%s,%s,%s,%s,%s

                )

                """,
                (

                    task_id,

                    "PICKING",

                    warehouse_id,

                    order_id,

                    product_id,

                    quantity,

                    "HIGH",

                    "CREATED",

                    worker_id,

                    random.randint(5,20),

                    now,

                    now,

                    "WMS",

                    correlation_id,

                    allocation_id,

                    quantity

                )

            )



            # -----------------------------------------
            # Worker busy
            # -----------------------------------------

            db.execute(
                """

                UPDATE workers

                SET current_status='BUSY'

                WHERE worker_id=%s


                """,
                (
                    worker_id,
                )
            )



            # -----------------------------------------
            # Event
            # -----------------------------------------

            payload = {


                "eventType":
                    EVENT_NAME,


                "pickingTask":

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


                    "quantity":
                        quantity,


                    "workerId":
                        worker_id,


                    "status":
                        "CREATED"

                },


                "occurredAt":
                    now.isoformat(),


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



            log_event_success(

                EVENT_NAME,

                {

                    "task_id":
                        task_id,


                    "allocation_id":
                        allocation_id,


                    "product_id":
                        product_id,


                    "quantity":
                        quantity

                }

            )



            created_tasks.append(

                {

                    "task_id":
                        task_id,


                    "allocation_id":
                        allocation_id,


                    "quantity":
                        quantity

                }

            )



        return created_tasks





if __name__=="__main__":


    try:


        if len(sys.argv)<2:

            raise Exception(
                """
Usage:

python picking_task_created.py ALLOC-000000001

or

python picking_task_created.py ORD-000000001
"""
            )


        generate_picking_task_created(
            sys.argv[1]
        )



    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise