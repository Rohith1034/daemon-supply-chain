from datetime import datetime, timezone
import uuid


from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "PackingTaskCreated"



def generate_packing_task_created():

    with Database() as db:


        # -------------------------------------------------
        # Find completed picking task
        # -------------------------------------------------

        picking_task = db.fetch_one(

            """

            SELECT

                task_id,
                order_id,
                warehouse_id,
                product_id,
                quantity,
                location,
                correlation_id


            FROM warehouse_tasks


            WHERE task_type='PICKING'


            AND status='COMPLETED'


            ORDER BY task_completed_at


            LIMIT 1


            FOR UPDATE


            """

        )



        if not picking_task:


            raise Exception(

                "No completed picking task found"

            )



        picking_task_id = picking_task["task_id"]

        order_id = picking_task["order_id"]

        warehouse_id = picking_task["warehouse_id"]

        product_id = picking_task["product_id"]

        quantity = picking_task["quantity"]



        if not order_id:


            raise Exception(

                "Picking task missing order_id"

            )



        # -------------------------------------------------
        # Duplicate packing check
        # -------------------------------------------------

        existing = db.fetch_one(

            """

            SELECT

                task_id


            FROM warehouse_tasks


            WHERE task_type='PACKING'


            AND order_id=%s


            AND status IN

            (

                'CREATED',

                'STARTED',

                'COMPLETED'

            )


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

                Task:
                {existing['task_id']}

                """

            )



        # -------------------------------------------------
        # Find packing worker
        # -------------------------------------------------

        worker = db.fetch_one(

            """

            SELECT

                worker_id


            FROM workers


            WHERE warehouse_id=%s


            AND current_status='AVAILABLE'


            AND role IN

            (

                'PACKER',

                'WAREHOUSE_ASSOCIATE'

            )


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

                "No packing worker available"

            )



        worker_id = worker["worker_id"]



        # -------------------------------------------------
        # Create task
        # -------------------------------------------------

        task_id = next_task_id(db)


        now = datetime.now(
            timezone.utc
        )



        correlation_id = (

            str(
                picking_task["correlation_id"]
            )

            if picking_task["correlation_id"]

            else str(uuid.uuid4())

        )



        db.execute(

            """

            INSERT INTO warehouse_tasks

            (

                task_id,

                task_type,

                warehouse_id,

                order_id,

                product_id,

                location,

                quantity,

                expected_quantity,

                priority,

                status,

                assigned_worker_id,

                estimated_minutes,

                created_at,

                assigned_at,

                created_by,

                correlation_id


            )


            VALUES

            (

                %s,%s,%s,%s,%s,%s,

                %s,%s,%s,%s,%s,%s,

                %s,%s,%s,%s

            )


            """,

            (

                task_id,

                "PACKING",

                warehouse_id,

                order_id,

                product_id,

                picking_task["location"],

                quantity,

                quantity,

                "HIGH",

                "CREATED",

                worker_id,

                15,

                now,

                now,

                "WMS",

                correlation_id

            )

        )



        # -------------------------------------------------
        # Lock worker
        # -------------------------------------------------

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



        # -------------------------------------------------
        # Event
        # -------------------------------------------------

        payload = {


            "eventType":

                EVENT_NAME,


            "occurredAt":

                now.isoformat(),



            "packingTask":

            {


                "taskId":

                    task_id,


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


                "order_id":

                    order_id,


                "product_id":

                    product_id,


                "warehouse_id":

                    warehouse_id,


                "worker_id":

                    worker_id,


                "correlation_id":

                    correlation_id


            }

        )



        return {


            "task_id":

                task_id


        }




if __name__=="__main__":


    try:


        generate_packing_task_created()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise