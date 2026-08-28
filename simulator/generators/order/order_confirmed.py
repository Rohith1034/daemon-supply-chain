from datetime import datetime, timezone, timedelta

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_paid_order(cursor):

    cursor.execute(
        """
        SELECT
            order_id,
            customer_id,
            warehouse_id,
            total_amount

        FROM orders

        WHERE order_status='PAYMENT_COMPLETED'

        ORDER BY random()

        LIMIT 1

        """
    )

    return cursor.fetchone()



def confirm_order():

    conn = get_connection()

    cursor = conn.cursor()


    try:


        order = get_paid_order(cursor)


        if not order:

            raise Exception(
                "No paid orders available"
            )



        order_id = order[0]

        customer_id = order[1]

        warehouse_id = order[2]

        amount = float(order[3])



        now = datetime.now(
            timezone.utc
        )



        promised_delivery = (

            now +

            timedelta(
                days=3
            )

        )



        #
        # Update order
        #

        cursor.execute(

        """

        UPDATE orders

        SET

        order_status='CONFIRMED',

        confirmed_at=%s,

        promised_delivery_date=%s


        WHERE order_id=%s

        """,

        (

        now,

        promised_delivery,

        order_id

        )

        )



        correlation_id = (

            generate_correlation_id(
                order_id,
                prefix="ORDER"
            )

        )



        event = {


        "event_id":

            generate_event_id(),



        "event_type":

            "OrderConfirmed",



        "event_version":

            "1.0",



        "timestamp":

            now.isoformat(),



        "source":

            "order-management-service",



        "aggregate_type":

            "ORDER",



        "aggregate_id":

            order_id,



        "correlation_id":

            correlation_id,



        "order_confirmation":{


            "order_id":

                order_id,


            "customer_id":

                customer_id,


            "warehouse_id":

                warehouse_id,


            "amount":

                amount,


            "status":

                "CONFIRMED",


            "promised_delivery_date":

                promised_delivery.isoformat()


        }


        }



        #
        # Outbox
        #

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



        conn.commit()


        print(
            "Confirmed:",
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

    confirm_order()