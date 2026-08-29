from datetime import datetime, timezone
import uuid
import random


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "PackingCompleted"



def next_package_id(db):

    value = db.fetch_one(
        """
        SELECT COUNT(*) + 1 AS id
        FROM packages
        """
    )

    return f"PKG-{int(value['id']):09d}"



def generate_packing_completed():


    with Database() as db:


        # ------------------------------------
        # Find STARTED packing task
        # ------------------------------------

        task = db.fetch_one(

            """
            SELECT *
            FROM warehouse_tasks
            WHERE task_type='PACKING'
            AND status='STARTED'
            ORDER BY task_started_at
            LIMIT 1
            """

        )


        if not task:

            raise Exception(
                "No STARTED packing task found"
            )



        task_id = task["task_id"]

        warehouse_id = task["warehouse_id"]

        worker_id = task["assigned_worker_id"]

        correlation_id = task["correlation_id"]

        now = datetime.now(
            timezone.utc
        )



        # ------------------------------------
        # Get order items
        # ------------------------------------

        items = db.fetch_all(

            """
            SELECT
                oi.product_id,
                oi.quantity
            FROM order_items oi
            JOIN inventory_allocations ia
                ON ia.product_id = oi.product_id
            WHERE oi.order_id = %s
            """,

            (
                task["shipment_id"],
            )

        )


        # fallback using allocation

        if not items:

            items = db.fetch_all(

                """
                SELECT
                    product_id,
                    allocated_quantity AS quantity
                FROM inventory_allocations
                WHERE order_id = (
                    SELECT order_id
                    FROM inventory_allocations
                    WHERE correlation_id=%s
                    LIMIT 1
                )
                """,

                (
                    correlation_id,
                )

            )



        if not items:

            raise Exception(
                "No items found for packing"
            )



        total_quantity = sum(
            x["quantity"]
            for x in items
        )



        # ------------------------------------
        # Create package
        # ------------------------------------

        package_id = next_package_id(db)



        db.execute(

            """
            INSERT INTO packages
            (
                package_id,
                order_id,
                warehouse_id,
                package_type,
                total_items,
                total_quantity,
                weight_kg,
                length_cm,
                width_cm,
                height_cm,
                package_status,
                packed_by,
                packed_at,
                correlation_id
            )

            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s
            )

            """,

            (

                package_id,

                db.fetch_one(
                    """
                    SELECT order_id
                    FROM inventory_allocations
                    WHERE correlation_id=%s
                    LIMIT 1
                    """,
                    (
                        correlation_id,
                    )
                )["order_id"],


                warehouse_id,

                "BOX",

                len(items),

                total_quantity,

                round(random.uniform(1,10),2),

                30,

                20,

                15,

                "PACKED",

                worker_id,

                now,

                correlation_id

            )

        )



        # ------------------------------------
        # Package Items
        # ------------------------------------

        for item in items:


            db.execute(

                """
                INSERT INTO package_items
                (
                    package_id,
                    product_id,
                    quantity
                )

                VALUES
                (
                    %s,%s,%s
                )

                """,

                (

                    package_id,

                    item["product_id"],

                    item["quantity"]

                )

            )



        # ------------------------------------
        # Complete task
        # ------------------------------------

        db.execute(

            """
            UPDATE warehouse_tasks

            SET

                status='COMPLETED',

                task_completed_at=%s,

                completed_by=%s,

                actual_minutes =
                    EXTRACT(
                        MINUTE
                        FROM
                        (%s-task_started_at)
                    )

            WHERE task_id=%s

            """,

            (

                now,

                worker_id,

                now,

                task_id

            )

        )



        # ------------------------------------
        # Free worker
        # ------------------------------------

        db.execute(

            """
            UPDATE workers

            SET current_status='AVAILABLE'

            WHERE worker_id=%s

            """,

            (
                worker_id,
            )

        )



        # ------------------------------------
        # Event Payload
        # ------------------------------------

        payload = {


            "eventType": EVENT_NAME,


            "occurredAt":
                now.isoformat(),


            "package":

            {

                "packageId":
                    package_id,

                "taskId":
                    task_id,

                "warehouseId":
                    warehouse_id,

                "workerId":
                    worker_id,

                "totalItems":
                    len(items),

                "totalQuantity":
                    total_quantity,

                "status":
                    "PACKED"

            },


            "correlationId":
                str(correlation_id)

        }



        # ------------------------------------
        # Publish
        # ------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="PACKAGE",

            aggregate_id=package_id,

            correlation_id=str(
                correlation_id
            ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

                "package_id":
                    package_id,

                "task_id":
                    task_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "quantity":
                    total_quantity,

                "correlation_id":
                    correlation_id

            }

        )





if __name__ == "__main__":


    try:

        generate_packing_completed()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise