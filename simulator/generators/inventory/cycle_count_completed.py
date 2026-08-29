from datetime import datetime, timezone
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "CycleCountCompleted"



def generate_cycle_count_completed():


    with Database() as db:


        # ----------------------------------------
        # Find started cycle count task
        # ----------------------------------------

        task = db.fetch_one(

            """

            SELECT

                task_id,
                task_type,
                warehouse_id,
                product_id,
                location,
                quantity,
                assigned_worker_id,
                correlation_id


            FROM warehouse_tasks


            WHERE task_type='CYCLE_COUNT'

            AND status='STARTED'


            ORDER BY task_started_at


            LIMIT 1


            """

        )


        if not task:

            raise Exception(
                "No STARTED cycle count task found"
            )


        task_id = task["task_id"]

        worker_id = task["assigned_worker_id"]

        warehouse_id = task["warehouse_id"]

        product_id = task["product_id"]

        location_id = task["location"]



        # ----------------------------------------
        # Get expected inventory
        # ----------------------------------------

        inventory = db.fetch_one(

            """

            SELECT

                on_hand_quantity


            FROM inventory


            WHERE product_id=%s

            AND warehouse_id=%s

            AND location_id=%s


            """,

            (

                product_id,

                warehouse_id,

                location_id

            )

        )


        if not inventory:

            raise Exception(
                "Inventory record not found"
            )



        expected_quantity = inventory["on_hand_quantity"]



        # ----------------------------------------
        # Generate physical count
        # ----------------------------------------

        variance = random.choice(

            [

                0,

                0,

                0,

                -1,

                1

            ]

        )


        actual_quantity = max(

            0,

            expected_quantity + variance

        )


        discrepancy = (

            actual_quantity

            -

            expected_quantity

        )



        now = datetime.now(

            timezone.utc

        )



        working_minutes = random.randint(

            10,

            30

        )



        # ----------------------------------------
        # Update warehouse task
        # ----------------------------------------

        db.execute(

            """

            UPDATE warehouse_tasks


            SET

                status='COMPLETED',

                received_quantity=%s,

                discrepancy_quantity=%s,

                actual_minutes=%s,

                task_completed_at=%s,

                completed_at=%s,

                completed_by=%s


            WHERE task_id=%s


            """,

            (

                actual_quantity,

                discrepancy,

                working_minutes,

                now,

                now,

                worker_id,

                task_id

            )

        )



        # ----------------------------------------
        # Update inventory if mismatch
        # ----------------------------------------

       


        # ----------------------------------------
        # Worker available again
        # ----------------------------------------

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



        # ----------------------------------------
        # Productivity calculation
        # ----------------------------------------

        accuracy_score = round(

            (

                min(
                    expected_quantity,
                    actual_quantity
                )

                /

                max(
                    expected_quantity,
                    1
                )

            )

            *

            100,

            2

        )


        productivity_score = round(

            (

                accuracy_score * 0.7

            )

            +

            (

                random.randint(70,100) * 0.3

            ),

            2

        )



        db.execute(

            """

            INSERT INTO worker_productivity

            (

                worker_id,

                task_id,

                task_type,

                warehouse_id,

                units_processed,

                working_minutes,

                accuracy_score,

                productivity_score,

                correlation_id


            )


            VALUES

            (

                %s,%s,%s,%s,%s,%s,%s,%s,%s

            )


            """,

            (

                worker_id,

                task_id,

                "CYCLE_COUNT",

                warehouse_id,

                expected_quantity,

                working_minutes,

                accuracy_score,

                productivity_score,

                task["correlation_id"]

            )

        )



        # ----------------------------------------
        # Event Payload
        # ----------------------------------------

        payload = {


            "eventType":

                EVENT_NAME,


            "occurredAt":

                now.isoformat(),


            "cycleCount":


            {

                "taskId":

                    task_id,


                "warehouseId":

                    warehouse_id,


                "locationId":

                    location_id,


                "productId":

                    product_id,


                "workerId":

                    worker_id,


                "expectedQuantity":

                    expected_quantity,


                "actualQuantity":

                    actual_quantity,


                "discrepancy":

                    discrepancy,


                "status":

                    "COMPLETED"

            },


            "correlationId":

                str(
                    task["correlation_id"]
                )

        }



        # ----------------------------------------
        # Publish event
        # ----------------------------------------

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



        # ----------------------------------------
        # Output
        # ----------------------------------------

        log_event_success(

            EVENT_NAME,

            {


                "task_id":

                    task_id,


                "warehouse_id":

                    warehouse_id,


                "location_id":

                    location_id,


                "product_id":

                    product_id,


                "expected_quantity":

                    expected_quantity,


                "actual_quantity":

                    actual_quantity,


                "discrepancy":

                    discrepancy,


                "worker_id":

                    worker_id,


                "correlation_id":

                    task["correlation_id"]

            }

        )




if __name__ == "__main__":


    try:

        generate_cycle_count_completed()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise