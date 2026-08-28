import random

from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)


def get_reserved_order(cursor):

    cursor.execute(
        """
        SELECT
            reservation_id,
            order_id,
            warehouse_id

        FROM inventory_reservations

        WHERE reservation_status='RESERVED'

        ORDER BY random()

        LIMIT 1

        FOR UPDATE SKIP LOCKED
        """
    )

    return cursor.fetchone()



def get_reserved_items(cursor, order_id):

    cursor.execute(
        """
        SELECT

        product_id,
        quantity

        FROM inventory_reservations

        WHERE order_id=%s

        """,
        (
            order_id,
        )
    )

    return cursor.fetchall()



def create_allocation():


    conn=get_connection()

    cursor=conn.cursor()


    try:


        order=get_reserved_order(
            cursor
        )


        if not order:

            raise Exception(
                "No reserved inventory"
            )



        order_id=order[1]

        warehouse_id=order[2]



        items=get_reserved_items(
            cursor,
            order_id
        )



        now=datetime.now(
            timezone.utc
        )


        allocations=[]



        for item in items:


            product_id=item[0]

            quantity=item[1]



            allocation_id=(

                f"ALLOC-{random.randint(1,99999999):08d}"

            )



            cursor.execute(

            """

            INSERT INTO inventory_allocations

            (

            allocation_id,

            order_id,

            warehouse_id,

            product_id,

            allocated_quantity,

            allocation_status

            )

            VALUES

            (%s,%s,%s,%s,%s,%s)

            """,

            (

            allocation_id,

            order_id,

            warehouse_id,

            product_id,

            quantity,

            "ALLOCATED"

            )

            )



            allocations.append(

            {

            "allocation_id":
                allocation_id,


            "product_id":
                product_id,


            "quantity":
                quantity

            }

            )



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

            "AllocationCreated",


        "event_version":

            "1.0",


        "timestamp":

            now.isoformat(),


        "source":

            "inventory-service",


        "aggregate_type":

            "ORDER",


        "aggregate_id":

            order_id,


        "correlation_id":

            correlation_id,


        "allocation":{


            "order_id":

                order_id,


            "warehouse_id":

                warehouse_id,


            "allocations":

                allocations,


            "status":

                "ALLOCATED"

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

        "ORDER",

        order_id,

        correlation_id,

        Json(event)

        )

        )

        cursor.execute(
            """
            UPDATE inventory_reservations
            SET reservation_status='ALLOCATED'
            WHERE order_id=%s
            AND reservation_status='RESERVED'
            """,
            (order_id,)
        )



        conn.commit()


        print(
            "Allocation created:",
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

    create_allocation()