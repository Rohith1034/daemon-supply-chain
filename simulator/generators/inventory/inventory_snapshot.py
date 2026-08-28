from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def create_inventory_snapshot():

    conn = get_connection()

    cursor = conn.cursor()


    try:


        cursor.execute(
            """
            SELECT

            product_id,
            warehouse_id,
            on_hand_quantity,
            reserved_quantity,
            damaged_quantity,
            available_quantity,
            safety_stock,
            reorder_point


            FROM inventory

            """
        )


        inventory_rows = cursor.fetchall()



        if not inventory_rows:

            print(
                "Inventory empty"
            )

            return



        now = datetime.now(
            timezone.utc
        )


        snapshot_items=[]



        for row in inventory_rows:


            (
                product_id,
                warehouse_id,
                on_hand,
                reserved,
                damaged,
                available,
                safety_stock,
                reorder_point

            ) = row



            #
            # Store snapshot
            #

            cursor.execute(

            """

            INSERT INTO inventory_snapshots

            (

            product_id,

            warehouse_id,

            on_hand_quantity,

            reserved_quantity,

            damaged_quantity,

            available_quantity,

            safety_stock,

            reorder_point,

            snapshot_time

            )


            VALUES

            (%s,%s,%s,%s,%s,%s,%s,%s,%s)


            """,

            (

            product_id,

            warehouse_id,

            on_hand,

            reserved,

            damaged,

            available,

            safety_stock,

            reorder_point,

            now

            )

            )




            snapshot_items.append(

            {

            "product_id":
                product_id,

            "warehouse_id":
                warehouse_id,

            "available_quantity":
                available

            }

            )




        correlation_id = generate_correlation_id(
            "SNAPSHOT",
            prefix="INVENTORY"
        )



        event = {


        "event_id":
            generate_event_id(),


        "event_type":
            "InventorySnapshot",


        "event_version":
            "1.0",


        "timestamp":
            now.isoformat(),


        "source":
            "inventory-service",


        "aggregate_type":
            "INVENTORY",


        "aggregate_id":
            "INVENTORY-SNAPSHOT",


        "correlation_id":
            correlation_id,


        "snapshot":{


            "warehouse_count":
                len(
                    set(
                    x["warehouse_id"]
                    for x in snapshot_items
                    )
                ),


            "sku_count":
                len(snapshot_items),


            "items":
                snapshot_items

        }


        }



        cursor.execute(

        """

        INSERT INTO event_outbox

        (

        event_id,

        event_type,

        aggregate_type,

        aggregate_id,

        correlation_id,

        payload

        )

        VALUES

        (%s,%s,%s,%s,%s,%s)

        """,

        (

        event["event_id"],

        event["event_type"],

        "INVENTORY",

        "INVENTORY-SNAPSHOT",

        correlation_id,

        Json(event)

        )

        )



        conn.commit()


        print(
            "Inventory snapshot created:",
            len(snapshot_items),
            "SKUs"
        )


        return event



    except Exception as e:

        conn.rollback()

        raise e


    finally:

        cursor.close()

        conn.close()



if __name__=="__main__":

    create_inventory_snapshot()