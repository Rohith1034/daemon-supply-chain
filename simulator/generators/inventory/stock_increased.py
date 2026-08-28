import random

from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_putaway_event(cursor):

    cursor.execute(
        """
        SELECT id, payload

        FROM event_outbox

        WHERE event_type='InventoryPutaway'

        AND status='PENDING'

        ORDER BY created_at DESC

        LIMIT 1

        FOR UPDATE SKIP LOCKED

        """
    )

    return cursor.fetchone()



def create_stock_increased():


    conn = get_connection()

    cursor = conn.cursor()


    try:


        record = get_putaway_event(cursor)


        if not record:

            print(
                "No InventoryPutaway event found"
            )

            return



        source_event_id = record[0]
        putaway_event = record[1]


        putaway = putaway_event[
            "putaway"
        ]


        warehouse_id = (
            putaway["warehouse_id"]
        )


        shipment_id = (
            putaway["shipment_id"]
        )

        po_id = putaway["po_id"]



        now = datetime.now(
            timezone.utc
        )



        inventory_updates=[]



        for item in putaway["items"]:


            product_id = item["product_id"]

            quantity = item["quantity"]



            #
            # Check inventory exists
            #

            cursor.execute(

                """

                SELECT inventory_id

                FROM inventory

                WHERE product_id=%s

                AND warehouse_id=%s

                """,

                (

                product_id,

                warehouse_id

                )

            )


            existing = cursor.fetchone()



            if existing:


                cursor.execute(

                """

                UPDATE inventory

                SET

                on_hand_quantity =
                on_hand_quantity + %s,

                available_quantity =
                COALESCE(
                    available_quantity,
                    on_hand_quantity - reserved_quantity
                ) + %s,
                last_updated_at = %s

                WHERE inventory_id=%s

                """,

                (

                quantity,

                quantity,

                now,

                existing[0]

                )

                )


            else:


                safety_stock=random.randint(
                    50,
                    500
                )


                reorder_point=int(
                    safety_stock * 1.5
                )


                reorder_quantity=int(
                    safety_stock * 2
                )



                cursor.execute(

                """

                INSERT INTO inventory

                (

                product_id,

                warehouse_id,

                on_hand_quantity,

                reserved_quantity,

                available_quantity,

                damaged_quantity,

                safety_stock,

                reorder_point,

                reorder_quantity,

                last_updated_at

                )


                VALUES

                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)


                """,

                (

                product_id,

                warehouse_id,

                quantity,

                0,

                quantity,

                0,

                safety_stock,

                reorder_point,

                reorder_quantity,

                now

                )

                )



            inventory_updates.append(

            {

            "product_id":
                product_id,


            "warehouse_id":
                warehouse_id,


            "quantity_added":
                quantity

            }

            )



        correlation_id = (
            generate_correlation_id(
                po_id,
                prefix="PO"
            )
        )



        event={


        "event_id":
            generate_event_id(),


        "event_type":
            "StockIncreased",


        "event_version":
            "1.0",


        "timestamp":
            now.isoformat(),


        "source":
            "inventory-service",


        "aggregate_type":
            "INVENTORY",


        "aggregate_id":
            shipment_id,


        "correlation_id":
            correlation_id,


        "inventory_change":

        {


        "warehouse_id":
            warehouse_id,


        "reason":
            "GOODS_RECEIVED",


        "reference":
            shipment_id,


        "items":
            inventory_updates


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

        shipment_id,

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
            (
                source_event_id,
            )
        )



        conn.commit()


        print(
            "Stock increased successfully"
        )


        return event



    except Exception as e:


        conn.rollback()

        raise e



    finally:

        cursor.close()

        conn.close()



if __name__=="__main__":

    create_stock_increased()