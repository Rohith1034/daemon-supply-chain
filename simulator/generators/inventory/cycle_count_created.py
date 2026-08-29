from datetime import datetime, timezone
import uuid
import random


from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "CycleCountCreated"



def generate_cycle_count_created():

    with Database() as db:


        # -------------------------------------------------
        # Find inventory requiring cycle count
        # -------------------------------------------------

        inventory = db.fetch_one(

            """

            SELECT

                inventory_id,
                product_id,
                warehouse_id,
                location_id,
                on_hand_quantity


            FROM inventory


            WHERE location_id IS NOT NULL


            AND inventory_status='RECEIVED'


            ORDER BY last_updated_at ASC


            LIMIT 1


            FOR UPDATE


            """

        )



        if not inventory:


            raise Exception(

                "No inventory available for cycle count"

            )



        inventory_id = inventory["inventory_id"]

        product_id = inventory["product_id"]

        warehouse_id = inventory["warehouse_id"]

        location_id = inventory["location_id"]

        expected_quantity = inventory["on_hand_quantity"]



        # -------------------------------------------------
        # Duplicate cycle count check
        # -------------------------------------------------

        existing = db.fetch_one(

            """

            SELECT

                task_id


            FROM warehouse_tasks


            WHERE task_type='CYCLE_COUNT'


            AND product_id=%s


            AND location=%s


            AND status IN

            (

                'CREATED',

                'STARTED'

            )


            LIMIT 1


            """,

            (

                product_id,

                location_id

            )

        )



        if existing:


            raise Exception(

                f"""

                Cycle count already exists

                Task:
                {existing['task_id']}

                """

            )



        # -------------------------------------------------
        # Assign worker
        # -------------------------------------------------

        worker = db.fetch_one(

            """

            SELECT

                worker_id


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

                "No available worker"

            )



        worker_id = worker["worker_id"]



        # -------------------------------------------------
        # Create task
        # -------------------------------------------------

        task_id = next_task_id(db)



        now = datetime.now(
            timezone.utc
        )



        correlation_id = str(
            uuid.uuid4()
        )



        estimated_minutes = random.randint(
            10,
            30
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

                expected_quantity,

                counted_quantity,

                priority,

                status,

                assigned_worker_id,

                estimated_minutes,

                created_at,

                created_by,

                assigned_at,

                correlation_id


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

                "CYCLE_COUNT",

                warehouse_id,

                product_id,

                location_id,

                expected_quantity,

                expected_quantity,

                None,

                "MEDIUM",

                "CREATED",

                worker_id,

                estimated_minutes,

                now,

                "WMS",

                now,

                correlation_id

            )

        )



        # -------------------------------------------------
        # Mark worker busy
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
        # Event Payload
        # -------------------------------------------------

        payload = {


            "eventType":

                EVENT_NAME,


            "occurredAt":

                now.isoformat(),



            "cycleCountTask":

            {


                "taskId":

                    task_id,


                "inventoryId":

                    inventory_id,


                "warehouseId":

                    warehouse_id,


                "locationId":

                    location_id,


                "productId":

                    product_id,


                "expectedQuantity":

                    expected_quantity,


                "workerId":

                    worker_id,


                "status":

                    "CREATED"


            },


            "correlationId":

                correlation_id


        }



        # -------------------------------------------------
        # Outbox
        # -------------------------------------------------

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


                "inventory_id":

                    inventory_id,


                "product_id":

                    product_id,


                "warehouse_id":

                    warehouse_id,


                "location_id":

                    location_id,


                "worker_id":

                    worker_id,


                "expected_quantity":

                    expected_quantity,


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

        generate_cycle_count_created()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise