from datetime import datetime, timezone
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "InventoryReceived"


def generate_inventory_received():

    with Database() as db:


        # -----------------------------------
        # Find completed receiving shipment
        # -----------------------------------

        shipment = db.fetch_one(

            """
            SELECT *

            FROM shipments

            WHERE shipment_status='ARRIVED'

            ORDER BY actual_delivery

            LIMIT 1

            """

        )


        if not shipment:

            raise Exception(
                "No ARRIVED shipment found"
            )


        shipment_id = shipment["shipment_id"]



        # -----------------------------------
        # Shipment items
        # -----------------------------------

        items = db.fetch_all(

            """

            SELECT *

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



        received_items=[]


        now=datetime.now(
            timezone.utc
        )



        # -----------------------------------
        # Update inventory
        # -----------------------------------

        for item in items:


            shipped_quantity = item[
                "shipped_quantity"
            ]


            damaged_quantity=random.randint(

                0,

                max(
                    1,
                    int(
                        shipped_quantity * 0.02
                    )
                )

            )


            received_quantity=(

                shipped_quantity

                -

                damaged_quantity

            )



            inventory=db.fetch_one(

                """

                SELECT *

                FROM inventory

                WHERE product_id=%s

                AND warehouse_id=%s

                """,

                (

                    item["product_id"],

                    shipment["warehouse_id"]

                )

            )



            if inventory:


                db.execute(

                    """

                    UPDATE inventory

                    SET

                    on_hand_quantity =
                    on_hand_quantity + %s,


                    damaged_quantity =
                    damaged_quantity + %s,


                    last_updated_at=%s


                    WHERE inventory_id=%s


                    """,

                    (

                        received_quantity,

                        damaged_quantity,

                        now,

                        inventory["inventory_id"]

                    )

                )



            else:


                db.execute(

                    """

                    INSERT INTO inventory

                    (

                    product_id,

                    warehouse_id,

                    on_hand_quantity,

                    damaged_quantity

                    )


                    VALUES

                    (%s,%s,%s,%s)

                    """,

                    (

                        item["product_id"],

                        shipment["warehouse_id"],

                        received_quantity,

                        damaged_quantity

                    )

                )



            received_items.append(

                {

                "product_id":
                    item["product_id"],


                "received_quantity":
                    received_quantity,


                "damaged_quantity":
                    damaged_quantity

                }

            )



        # -----------------------------------
        # Shipment received
        # -----------------------------------

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



        # -----------------------------------
        # Payload
        # -----------------------------------

        payload={


            "eventType":
                EVENT_NAME,


            "occurredAt":
                now.isoformat(),


            "shipment":


            {

                "shipmentId":
                    shipment_id,


                "warehouseId":
                    shipment["warehouse_id"],


                "supplierId":
                    shipment["supplier_id"],


                "items":
                    received_items,


                "status":
                    "RECEIVED"

            },


            "correlationId":

                str(
                    shipment["correlation_id"]
                )


        }



        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="INVENTORY",

            aggregate_id=shipment_id,

            correlation_id=str(
                shipment["correlation_id"]
            ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

            "shipment_id":
                shipment_id,


            "warehouse_id":
                shipment["warehouse_id"],


            "items":
                len(received_items)

            }

        )




if __name__=="__main__":


    try:

        generate_inventory_received()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise