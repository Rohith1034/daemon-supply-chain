import random
from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_reservation(cursor):

    cursor.execute("""

    SELECT
        reservation_id,
        order_id,
        product_id,
        warehouse_id,
        quantity

    FROM inventory_reservations

    WHERE reservation_status='RESERVED'

    ORDER BY random()

    LIMIT 1

    FOR UPDATE SKIP LOCKED

    """)


    return cursor.fetchone()



def release_inventory():


    conn=get_connection()
    cursor=conn.cursor()


    try:


        reservation=get_reservation(cursor)


        if not reservation:
            raise Exception(
                "No reservation found"
            )


        reservation_id=reservation[0]
        order_id=reservation[1]
        product_id=reservation[2]
        warehouse_id=reservation[3]
        quantity=reservation[4]


        now=datetime.now(
            timezone.utc
        )


        correlation_id=generate_correlation_id(
            order_id,
            prefix="ORDER"
        )


        cursor.execute("""

        UPDATE inventory_reservations

        SET
            reservation_status='RELEASED'

        WHERE reservation_id=%s

        """,
        (
            reservation_id,
        ))

        cursor.execute(
            """
            UPDATE inventory

            SET
                reserved_quantity = reserved_quantity - %s,
                available_quantity = COALESCE(
                    available_quantity,
                    on_hand_quantity - reserved_quantity
                ) + %s,
                last_updated_at = %s

            WHERE product_id=%s
            AND warehouse_id=%s
            """,
            (
                quantity,
                quantity,
                now,
                product_id,
                warehouse_id,
            )
        )



        event={

        "event_id":
            generate_event_id(),


        "event_type":
            "InventoryReleased",


        "timestamp":
            now.isoformat(),


        "correlation_id":
            correlation_id,


        "inventory":

        {

        "reservation_id":
            reservation_id,


        "product_id":
            product_id,


        "warehouse_id":
            warehouse_id,


        "released_quantity":
            quantity,


        "reason":
            random.choice(
            [
            "ORDER_CANCELLED",
            "PAYMENT_FAILED",
            "TIMEOUT"
            ])

        }

        }



        cursor.execute(
        """

        INSERT INTO event_outbox

        VALUES
        (
        DEFAULT,
        %s,
        %s,
        %s,
        %s,
        %s,
        NOW(),
        NULL,
        'PENDING'
        )

        """,

        (

        event["event_id"],

        "InventoryReleased",

        "INVENTORY",

        product_id,

        correlation_id,

        Json(event)

        ))


        conn.commit()

        print(
            "Released:",
            product_id
        )


    except Exception as e:

        conn.rollback()
        raise e


    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":
    release_inventory()