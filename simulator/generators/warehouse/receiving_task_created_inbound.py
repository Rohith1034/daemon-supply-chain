from datetime import datetime, timezone

from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ReceivingTaskCreated"



def generate_receiving_task_created():

    with Database() as db:


        # ------------------------------------
        # Find delivered shipment
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id,
                total_quantity
            FROM shipments
            WHERE shipment_status='DELIVERED'
            AND receiving_task_created=false
            ORDER BY updated_at
            LIMIT 1
            """
        )


        if not shipment:
            raise Exception(
                "No DELIVERED shipment found"
            )


        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )


        quantity = shipment["total_quantity"]


        now = datetime.now(
            timezone.utc
        )



        # ------------------------------------
        # Pick receiving worker
        # ------------------------------------

        worker = db.fetch_one(
            """
            SELECT
                worker_id
            FROM workers
            WHERE warehouse_id=%s
            AND current_status='AVAILABLE'
            ORDER BY random()
            LIMIT 1
            """,
            (
                warehouse_id,
            )
        )

        if not worker:
            raise Exception(
                "No available warehouse worker found for receiving"
            )

        worker_id = worker["worker_id"]

        # ------------------------------------
        # Generate task id
        # ------------------------------------

        task_id = next_task_id(db)



        dock = "DOCK-001"



        # ------------------------------------
        # Insert warehouse task
        # ------------------------------------

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
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s
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

                "SYSTEM",

                correlation_id,

                dock,

                quantity

            )
        )

        db.execute(
            """
            UPDATE shipments
            SET receiving_task_created=true
            WHERE shipment_id=%s
            """,
            (
                shipment_id,
            )
        )

        # ------------------------------------
        # Event payload
        # ------------------------------------

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
                    dock,


                "expectedQuantity":
                    quantity,


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

                "shipment_id":
                    shipment_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "expected_quantity":
                    quantity,

                "dock_location":
                    dock,

                "correlation_id":
                    correlation_id

            }

        )




if __name__ == "__main__":

    try:

        generate_receiving_task_created()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise