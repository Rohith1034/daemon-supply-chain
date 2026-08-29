from datetime import datetime, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
import uuid


EVENT_NAME = "GoodsReceived"



def generate_goods_received():

    with Database() as db:

        # ------------------------------------
        # Find started receiving task
        # ------------------------------------

        task = db.fetch_one(
            """
            SELECT
                task_id,
                shipment_id,
                warehouse_id,
                assigned_worker_id,
                expected_quantity,
                correlation_id
            FROM warehouse_tasks
            WHERE task_type='RECEIVING'
            AND status='STARTED'
            ORDER BY created_at
            LIMIT 1
            """
        )


        if not task:
            raise Exception(
                "No STARTED receiving task found"
            )


        task_id = task["task_id"]
        shipment_id = task["shipment_id"]
        warehouse_id = task["warehouse_id"]
        worker_id = task["assigned_worker_id"]

        quantity = task["expected_quantity"]

        correlation_id = str(
            task["correlation_id"]
        )


        now = datetime.now(
            timezone.utc
        )


        # ------------------------------------
        # Get shipment items
        # ------------------------------------

        items = db.fetch_all(
            """
            SELECT
                shipment_item_id,
                product_id,
                shipped_quantity
            FROM shipment_items
            WHERE shipment_id=%s
            """,
            (
                shipment_id,
            )
        )


        if not items:
            raise Exception(
                "No shipment items found"
            )



        # ------------------------------------
        # Update inventory
        # ------------------------------------

        for item in items:


            db.execute(
                """
                UPDATE shipment_items

                SET received_quantity=%s

                WHERE shipment_item_id=%s
                """,
                (
                    item["shipped_quantity"],
                    item["shipment_item_id"]
                )
            )

            inventory = db.fetch_one(
                """
                SELECT
                    inventory_id,
                    on_hand_quantity
                FROM inventory
                WHERE product_id=%s
                AND warehouse_id=%s
                FOR UPDATE
                """,
                (
                    item["product_id"],
                    warehouse_id
                )
            )

            # ------------------------------------
            # Create inventory if missing
            # ------------------------------------

            if not inventory:

                inventory = db.fetch_one(
                    """
                    INSERT INTO inventory
                    (
                        product_id,
                        warehouse_id,
                        on_hand_quantity,
                        reserved_quantity,
                        last_updated_at
                    )

                    VALUES
                    (
                        %s,%s,%s,%s,%s
                    )

                    RETURNING inventory_id

                    """,
                    (
                        item["product_id"],
                        warehouse_id,
                        item["shipped_quantity"],
                        0,
                        now
                    )
                )

                inventory_id = inventory["inventory_id"]


            else:

                inventory_id = inventory["inventory_id"]

                db.execute(
                    """
                    UPDATE inventory

                    SET
                        on_hand_quantity = on_hand_quantity + %s,
                        last_updated_at=%s

                    WHERE inventory_id=%s
                    """,

                    (
                        item["shipped_quantity"],
                        now,
                        inventory_id
                    )
                )

            # ------------------------------------
            # Always create inventory transaction
            # ------------------------------------

            db.execute(
                """
                INSERT INTO inventory_transactions
                (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    transaction_type,
                    quantity,
                    reference_type,
                    reference_id,
                    task_id,
                    shipment_id,
                    correlation_id
                )

                VALUES
                (
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s
                )
                """,
                (

                    inventory_id,

                    item["product_id"],

                    warehouse_id,

                    "STOCK_RECEIVED",

                    item["shipped_quantity"],

                    "SHIPMENT",

                    shipment_id,

                    task_id,

                    shipment_id,

                    correlation_id

                )
            )


        # ------------------------------------
        # Complete warehouse task
        # ------------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks

            SET
                status='COMPLETED',
                received_quantity=%s,
                completed_at=%s,
                task_completed_at=%s,
                completed_by=%s

            WHERE task_id=%s
            """,
            (
                quantity,
                now,
                now,
                worker_id,
                task_id
            )
        )



        # ------------------------------------
        # Update shipment
        # ------------------------------------

        db.execute(
            """
            UPDATE shipments

            SET
                shipment_status='RECEIVED',
                updated_at=%s

            WHERE shipment_id=%s
            """,
            (
                now,
                shipment_id
            )
        )



        # ------------------------------------
        # Event payload
        # ------------------------------------

        payload = {

            "eventType": EVENT_NAME,

            "occurredAt":
                now.isoformat(),


            "goodsReceived":

            {

                "taskId":
                    task_id,

                "shipmentId":
                    shipment_id,

                "warehouseId":
                    warehouse_id,

                "receivedQuantity":
                    quantity,

                "workerId":
                    worker_id,

                "status":
                    "RECEIVED"

            },


            "correlationId":
                correlation_id

        }



        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=correlation_id,

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

                "task_id": task_id,

                "shipment_id": shipment_id,

                "warehouse_id": warehouse_id,

                "received_quantity": quantity,

                "worker_id": worker_id,

                "correlation_id": correlation_id

            }

        )




if __name__ == "__main__":

    try:

        generate_goods_received()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise