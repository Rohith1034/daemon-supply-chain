import random

from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_inventory_received(cursor):

    cursor.execute(
        """
        SELECT id, payload

        FROM event_outbox

        WHERE event_type='InventoryReceived'

        AND status='PENDING'

        ORDER BY created_at DESC

        LIMIT 1

        FOR UPDATE SKIP LOCKED
        """
    )

    return cursor.fetchone()



def get_worker(cursor, warehouse_id):

    cursor.execute(
        """
        SELECT worker_id,
               role

        FROM workers

        WHERE warehouse_id=%s

        AND employment_status='Active'

        AND role IN
        (
        'Inventory Clerk',
        'Warehouse Associate'
        )

        ORDER BY random()

        LIMIT 1
        """,
        (
            warehouse_id,
        )
    )

    return cursor.fetchone()



def get_location(cursor, warehouse_id):


    cursor.execute(
        """
        SELECT
        location_id,
        zone,
        aisle,
        rack,
        bin

        FROM warehouse_locations

        WHERE warehouse_id=%s

        AND status='ACTIVE'

        ORDER BY random()

        LIMIT 1

        """,
        (
            warehouse_id,
        )
    )


    return cursor.fetchone()



def create_inventory_putaway():


    conn=get_connection()

    cursor=conn.cursor()


    try:


        record=get_inventory_received(cursor)


        if not record:

            print(
                "No InventoryReceived event"
            )

            return



        source_event_id=record[0]
        received_event=record[1]


        receipt=received_event[
            "goods_receipt"
        ]


        warehouse_id=receipt[
            "warehouse_id"
        ]


        shipment_id=receipt[
            "shipment_id"
        ]

        po_id=receipt[
            "po_id"
        ]



        worker=get_worker(
            cursor,
            warehouse_id
        )


        location=get_location(
            cursor,
            warehouse_id
        )



        if not worker:

            raise Exception(
                "No warehouse worker found"
            )


        if not location:

            raise Exception(
                "No warehouse location found"
            )



        now=datetime.now(
            timezone.utc
        )



        putaway_items=[]


        for item in receipt["items"]:


            putaway_items.append(

                {

                "product_id":
                    item["product_id"],


                "quantity":
                    item["received_quantity"],


                "location_id":
                    location[0]

                }

            )



        correlation_id=(
            generate_correlation_id(
                po_id,
                prefix="PO"
            )
        )



        task_id = (
            f"PUT-{shipment_id}"
        )



        event={


        "event_id":
            generate_event_id(),


        "event_type":
            "InventoryPutaway",


        "event_version":
            "1.0",


        "timestamp":
            now.isoformat(),


        "source":
            "warehouse-management-system",


        "aggregate_type":
            "WAREHOUSE_TASK",


        "aggregate_id":
            task_id,


        "correlation_id":
            correlation_id,



        "putaway":{


            "task_id":
                task_id,


            "shipment_id":
                shipment_id,

            "po_id":
                po_id,


            "warehouse_id":
                warehouse_id,


            "assigned_worker":{


                "worker_id":
                    worker[0],

                "role":
                    worker[1]

            },


            "destination_location":{


                "location_id":
                    location[0],


                "zone":
                    location[1],


                "aisle":
                    location[2],


                "rack":
                    location[3],


                "bin":
                    location[4]

            },


            "items":
                putaway_items,


            "status":
                "COMPLETED"


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

        "WAREHOUSE_TASK",

        task_id,

        correlation_id,

        Json(event)

        )

        )



        cursor.execute(
            """
            UPDATE event_outbox
            SET status='PROCESSED'
            WHERE id=%s
            """,
            (source_event_id,)
        )

        conn.commit()


        print(
            "Putaway completed:",
            task_id
        )


        return event



    except Exception as e:

        conn.rollback()

        raise e


    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":

    create_inventory_putaway()