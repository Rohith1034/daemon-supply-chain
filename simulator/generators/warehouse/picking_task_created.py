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


EVENT_NAME="PickingTaskCreated"



def generate_picking_task_created(allocation_id):


    with Database() as db:


        allocation=db.fetch_one(
            """

            SELECT

                allocation_id,
                order_id,
                product_id,
                warehouse_id,
                allocated_quantity,
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
Inventory allocation not found:

{allocation_id}
"""
            )



        warehouse_id=allocation["warehouse_id"]

        order_id=allocation["order_id"]

        product_id=allocation["product_id"]

        quantity=allocation["allocated_quantity"]

        correlation_id=str(
            allocation["correlation_id"]
        )


        existing=db.fetch_one(
            """

            SELECT task_id

            FROM warehouse_tasks

            WHERE task_type='PICKING'

            AND allocation_id=%s


            """,

            (
                allocation_id,
            )

        )


        if existing:

            raise Exception(
                f"""
Picking task already exists:

{existing['task_id']}
"""
            )



        worker=db.fetch_one(
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
                "No picking worker available"
            )


        worker_id=worker["worker_id"]


        now=datetime.now(
            timezone.utc
        )


        task_id=next_task_id(db)


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


        payload={

            "eventType":EVENT_NAME,

            "pickingTask":{

                "taskId":task_id,

                "allocationId":allocation_id,

                "orderId":order_id,

                "warehouseId":warehouse_id,

                "productId":product_id,

                "quantity":quantity,

                "workerId":worker_id,

                "status":"CREATED"

            },

            "occurredAt":now.isoformat(),

            "correlationId":correlation_id

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

            "task_id":task_id,

            "allocation_id":allocation_id

            }

        )


        return {
            "task_id":task_id
        }



if __name__=="__main__":

    try:

        generate_picking_task_created(
            sys.argv[1]
        )


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise