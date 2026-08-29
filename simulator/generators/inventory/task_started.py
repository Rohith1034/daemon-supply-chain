from datetime import datetime, timezone

from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "TaskStarted"



def generate_task_started():

    with Database() as db:


        # ------------------------------------------------
        # Find next pending warehouse task
        # ------------------------------------------------

        task = db.fetch_one(

            """

            SELECT

                task_id,
                task_type,
                shipment_id,
                warehouse_id,
                product_id,
                location,
                quantity,
                assigned_worker_id,
                correlation_id


            FROM warehouse_tasks


            WHERE status='CREATED'


            ORDER BY created_at


            LIMIT 1


            """

        )


        if not task:

            raise Exception(
                "No CREATED warehouse task found"
            )



        task_id = task["task_id"]

        worker_id = task["assigned_worker_id"]



        # ------------------------------------------------
        # Validate worker
        # ------------------------------------------------

        worker = db.fetch_one(

            """

            SELECT

                worker_id,
                current_status


            FROM workers


            WHERE worker_id=%s


            """,

            (

                worker_id,

            )

        )


        if not worker:

            raise Exception(
                f"Worker not found {worker_id}"
            )


        if worker["current_status"] != "AVAILABLE":

            raise Exception(

                f"Worker {worker_id} not available"

            )



        now = datetime.now(
            timezone.utc
        )



        # ------------------------------------------------
        # Update warehouse task
        # ------------------------------------------------

        db.execute(

            """

            UPDATE warehouse_tasks


            SET

                status='STARTED',

                task_started_at=%s,

                started_by=%s


            WHERE task_id=%s


            """,

            (

                now,

                worker_id,

                task_id

            )

        )



        # ------------------------------------------------
        # Update worker status
        # ------------------------------------------------

        db.execute(

            """

            UPDATE workers


            SET

                current_status='BUSY'


            WHERE worker_id=%s


            """,

            (

                worker_id,

            )

        )



        # ------------------------------------------------
        # Payload
        # ------------------------------------------------


        payload = {


            "eventType":

                EVENT_NAME,


            "occurredAt":

                now.isoformat(),


            "task":


            {


                "taskId":

                    task_id,


                "taskType":

                    task["task_type"],


                "shipmentId":

                    task["shipment_id"],


                "warehouseId":

                    task["warehouse_id"],


                "productId":

                    task["product_id"],


                "locationId":

                    task["location"],


                "quantity":

                    task["quantity"],


                "workerId":

                    worker_id,


                "status":

                    "STARTED",


                "startedAt":

                    now.isoformat()

            },


            "correlationId":

                str(
                    task["correlation_id"]
                )

        }



        # ------------------------------------------------
        # Publish event
        # ------------------------------------------------


        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="WAREHOUSE_TASK",

            aggregate_id=task_id,

            correlation_id=str(
                task["correlation_id"]
            ),

            payload=payload

        )



        # ------------------------------------------------
        # Console output
        # ------------------------------------------------


        log_event_success(

            EVENT_NAME,

            {


                "task_id":

                    task_id,


                "task_type":

                    task["task_type"],


                "warehouse_id":

                    task["warehouse_id"],


                "worker_id":

                    worker_id,


                "status":

                    "STARTED",


                "correlation_id":

                    task["correlation_id"]

            }

        )




if __name__ == "__main__":


    try:

        generate_task_started()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise