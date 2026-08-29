from datetime import datetime, timezone
import uuid


from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ReceivingTaskCreated"



def generate_receiving_task_created(shipment_id):


    with Database() as db:



        # -------------------------------------------------
        # Get arrived shipment
        # -------------------------------------------------

        shipment = db.fetch_one(

            """

            SELECT

                shipment_id,
                warehouse_id,
                correlation_id,
                shipment_status


            FROM shipments


            WHERE shipment_id=%s


            AND shipment_status='ARRIVED'


            FOR UPDATE


            """,

            (

                shipment_id,

            )

        )



        if not shipment:


            raise Exception(

                f"""

                ARRIVED shipment not found

                Shipment:
                {shipment_id}

                """

            )



        warehouse_id = shipment["warehouse_id"]



        # -------------------------------------------------
        # Duplicate receiving task check
        # -------------------------------------------------

        existing = db.fetch_one(

            """

            SELECT

                task_id


            FROM warehouse_tasks


            WHERE shipment_id=%s


            AND task_type='RECEIVING'


            LIMIT 1


            """,

            (

                shipment_id,

            )

        )


        if existing:


            raise Exception(

                f"""

                Receiving task already exists

                Task:
                {existing['task_id']}

                """

            )



        # -------------------------------------------------
        # Calculate shipment quantity
        # -------------------------------------------------

        quantity_result = db.fetch_one(

            """

            SELECT

                COALESCE(
                    SUM(shipped_quantity),
                    0
                ) AS total_quantity


            FROM shipment_items


            WHERE shipment_id=%s


            """,

            (

                shipment_id,

            )

        )


        quantity = quantity_result["total_quantity"]



        if quantity <= 0:


            raise Exception(

                "Shipment contains no items"

            )



        # -------------------------------------------------
        # Find receiving worker
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

                "No receiving worker available"

            )



        worker_id = worker["worker_id"]



        # -------------------------------------------------
        # Generate task
        # -------------------------------------------------

        task_id = next_task_id(db)



        now = datetime.now(
            timezone.utc
        )



        correlation_id = (

            str(
                shipment["correlation_id"]
            )

            if shipment["correlation_id"]

            else str(uuid.uuid4())

        )



        dock_location = "DOCK-001"



        # -------------------------------------------------
        # Insert receiving task
        # -------------------------------------------------

        db.execute(

            """

            INSERT INTO warehouse_tasks

            (

                task_id,

                task_type,

                warehouse_id,

                shipment_id,

                quantity,

                priority,

                status,

                assigned_worker_id,

                estimated_minutes,

                created_at,

                assigned_at,

                created_by,

                correlation_id,

                dock_location,

                expected_quantity


            )


            VALUES

            (

                %s,%s,%s,%s,%s,

                %s,%s,%s,%s,%s,

                %s,%s,%s,%s,%s

            )


            """,

            (

                task_id,

                "RECEIVING",

                warehouse_id,

                shipment_id,

                quantity,

                "HIGH",

                "CREATED",

                worker_id,

                30,

                now,

                now,

                "WMS",

                correlation_id,

                dock_location,

                quantity

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



            "receivingTask":

            {


                "taskId":

                    task_id,


                "shipmentId":

                    shipment_id,


                "warehouseId":

                    warehouse_id,


                "workerId":

                    worker_id,


                "dockLocation":

                    dock_location,


                "expectedQuantity":

                    quantity,


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


                "shipment_id":

                    shipment_id,


                "warehouse_id":

                    warehouse_id,


                "worker_id":

                    worker_id,


                "quantity":

                    quantity,


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


        generate_receiving_task_created(
            "SHIPMENT_ID"
        )


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise