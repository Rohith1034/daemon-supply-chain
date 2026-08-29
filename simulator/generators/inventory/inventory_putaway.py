from core.db import Database

from core.outbox import publish_event

from core.payloads import (
    build_inventory_putaway_payload
)

from core.logger import (
    log_event_success,
    log_event_failure
)

import uuid



EVENT_NAME="InventoryPutaway"



def generate_inventory_putaway():


    with Database() as db:



        # ---------------------------------
        # Find received inventory
        # ---------------------------------

        inventories=db.fetch_all(

            """

            SELECT *

            FROM inventory

            WHERE location_id IS NULL

            ORDER BY last_updated_at

            LIMIT 100

            """

        )



        if not inventories:


            raise Exception(

                "No inventory waiting for putaway"

            )



        putaway_items=[]



        correlation_id=str(
            uuid.uuid4()
        )



        for inv in inventories:



            # -----------------------------
            # Find bin location
            # -----------------------------

            location=db.fetch_one(

                """

                SELECT location_id

                FROM warehouse_locations

                WHERE warehouse_id=%s

                AND status='ACTIVE'

                ORDER BY random()

                LIMIT 1


                """,

                (

                    inv["warehouse_id"],

                )

            )



            if not location:


                raise Exception(

                    "No warehouse location found"

                )



            location_id=location[
                "location_id"
            ]



            # -----------------------------
            # Insert location stock
            # -----------------------------

            db.execute(

                """

                INSERT INTO inventory_locations

                (

                product_id,

                warehouse_id,

                location_id,

                quantity

                )


                VALUES

                (%s,%s,%s,%s)
                
                ON CONFLICT
                (product_id,warehouse_id,location_id)
                
                DO UPDATE SET
                
                quantity =
                inventory_locations.quantity 
                + EXCLUDED.quantity;

                """,

                (

                    inv["product_id"],

                    inv["warehouse_id"],

                    location_id,

                    inv["on_hand_quantity"]

                )

            )



            # -----------------------------
            # Update inventory
            # -----------------------------

            db.execute(

                """

                UPDATE inventory

                SET

                location_id=%s,

                last_updated_at=now()


                WHERE inventory_id=%s


                """,

                (

                    location_id,

                    inv["inventory_id"]

                )

            )



            putaway_items.append(

                {

                "product_id":
                    inv["product_id"],


                "warehouse_id":
                    inv["warehouse_id"],


                "quantity":
                    inv["on_hand_quantity"],


                "location_id":
                    location_id

                }

            )



        payload=build_inventory_putaway_payload(

            warehouse_id=
                inventories[0]["warehouse_id"],


            items=
                putaway_items,


            correlation_id=
                correlation_id

        )



        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="INVENTORY",

            aggregate_id=
                inventories[0]["warehouse_id"],


            correlation_id=
                correlation_id,


            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

            "warehouse_id":
                inventories[0]["warehouse_id"],


            "items":
                len(putaway_items),


            "correlation_id":
                correlation_id

            }

        )




if __name__=="__main__":


    try:

        generate_inventory_putaway()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise