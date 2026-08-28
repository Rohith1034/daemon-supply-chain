import random

from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_confirmed_order(cursor):

    cursor.execute(
        """
        SELECT
            order_id,
            warehouse_id

        FROM orders

        WHERE order_status='CONFIRMED'

        ORDER BY random()

        LIMIT 1

        FOR UPDATE SKIP LOCKED

        """
    )

    return cursor.fetchone()



def get_order_items(cursor, order_id):

    cursor.execute(

        """

        SELECT

        product_id,
        quantity

        FROM order_items

        WHERE order_id=%s

        """,

        (
            order_id,
        )

    )

    return cursor.fetchall()



def reserve_inventory():

    conn=get_connection()

    cursor=conn.cursor()


    try:


        order=get_confirmed_order(
            cursor
        )


        if not order:

            raise Exception(
                "No confirmed orders"
            )


        order_id=order[0]

        warehouse_id=order[1]



        items=get_order_items(

            cursor,

            order_id

        )



        now=datetime.now(
            timezone.utc
        )



        reserved_items=[]

        reservation_ids=[]



        for item in items:


            product_id=item[0]

            quantity=item[1]



            #

            cursor.execute(

            """

            SELECT

            available_quantity

            FROM inventory

            WHERE

            product_id=%s

            AND

            warehouse_id=%s


            FOR UPDATE

            """,

            (

            product_id,

            warehouse_id

            )

            )


            inventory=cursor.fetchone()



            if not inventory:

                raise Exception(
                    f"No inventory {product_id}"
                )



            available=inventory[0]



            if available < quantity:

                raise Exception(

                    f"Insufficient stock {product_id}"

                )



            #
            # Reserve stock
            #

            cursor.execute(

            """

            UPDATE inventory

            SET

            reserved_quantity =
            reserved_quantity + %s,

            available_quantity =
            COALESCE(available_quantity, on_hand_quantity - reserved_quantity) - %s,

            last_updated_at=NOW()


            WHERE

            product_id=%s

            AND

            warehouse_id=%s

            """,

            (

            quantity,

            quantity,

            product_id,

            warehouse_id

            )

            )



            reservation_id=(

                f"RES-{random.randint(1,99999999):08d}"

            )



            cursor.execute(

            """

            INSERT INTO inventory_reservations

            (

            reservation_id,

            order_id,

            product_id,

            warehouse_id,

            quantity,

            reservation_status

            )

            VALUES

            (%s,%s,%s,%s,%s,%s)

            """,

            (

            reservation_id,

            order_id,

            product_id,

            warehouse_id,

            quantity,

            "RESERVED"

            )

            )



            reserved_items.append(

            {

            "product_id":
                product_id,

            "quantity":
                quantity

            }

            )

            reservation_ids.append(reservation_id)



        correlation_id=(

            generate_correlation_id(
                order_id,
                prefix="ORDER"
            )

        )



        event={


        "event_id":

            generate_event_id(),



        "event_type":

            "InventoryReserved",



        "event_version":

            "1.0",



        "timestamp":

            now.isoformat(),



        "source":

            "inventory-service",



        "aggregate_type":

            "INVENTORY",



        "aggregate_id":

            reservation_ids[0],



        "correlation_id":

            correlation_id,



        "reservation":{


            "order_id":

                order_id,


            "warehouse_id":

                warehouse_id,


            "items":

                reserved_items,


            "status":

                "RESERVED"

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

        order_id,

        correlation_id,

        Json(event)

        )

        )

        cursor.execute(
            """
            UPDATE orders
            SET order_status='INVENTORY_RESERVED'
            WHERE order_id=%s
            """,
            (order_id,)
        )



        conn.commit()


        print(
            "Inventory reserved for:",
            order_id
        )


        return event



    except Exception as e:

        conn.rollback()

        raise e


    finally:

        cursor.close()

        conn.close()



if __name__=="__main__":

    reserve_inventory()