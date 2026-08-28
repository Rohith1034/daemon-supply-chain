import random
import uuid

from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_pending_order(cursor):

    cursor.execute(
        """
        SELECT
            order_id,
            customer_id,
            total_amount

        FROM orders

        WHERE order_status='CREATED'

        ORDER BY random()

        LIMIT 1

        """
    )

    return cursor.fetchone()



def create_payment_success():

    conn=get_connection()

    cursor=conn.cursor()


    try:


        order=get_pending_order(cursor)


        if not order:

            raise Exception(
                "No pending orders found"
            )



        order_id = order[0]

        customer_id = order[1]
        amount = float(order[2])


        payment_id=(

            f"PAY-{random.randint(1,99999999):08d}"

        )


        transaction_id=(

            "TXN-" +
            str(uuid.uuid4())
            .replace("-","")
            [:16]

        )



        now=datetime.now(
            timezone.utc
        )



        fraud_score=round(

            random.uniform(
                1,
                20
            ),

            2

        )



        payment_method=random.choice(

            [

            "CREDIT_CARD",

            "DEBIT_CARD",

            "UPI",

            "PAYPAL",

            "NET_BANKING"

            ]

        )



        #
        # Insert payment
        #

        cursor.execute(

        """

        INSERT INTO payments

        (

        payment_id,

        order_id,

        customer_id,

        payment_status,

        payment_method,

        payment_gateway,

        transaction_id,

        amount,

        currency,

        fraud_score,

        payment_time

        )


        VALUES

        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

        """,

        (

        payment_id,

        order_id,

        customer_id,

        "SUCCESS",

        payment_method,

        random.choice(

            [

            "STRIPE",

            "RAZORPAY",

            "PAYPAL"

            ]

        ),

        transaction_id,

        amount,

        "USD",

        fraud_score,

        now

        )

        )



        #
        # Update order
        #

        cursor.execute(

        """

        UPDATE orders

        SET order_status='PAYMENT_COMPLETED'

        WHERE order_id=%s

        """,

        (

        order_id,

        )

        )



        correlation_id = (
            generate_correlation_id(
                order_id,
                prefix="ORDER"
            )
        )



        event={


        "event_id":
            generate_event_id(),


        "event_type":
            "PaymentSucceeded",


        "event_version":
            "1.0",


        "timestamp":
            now.isoformat(),


        "source":
            "payment-service",


        "aggregate_type":
            "PAYMENT",


        "aggregate_id":
            payment_id,


        "correlation_id":
            correlation_id,


        "payment":{


            "payment_id":
                payment_id,


            "order_id":
                order_id,


            "customer_id":
                customer_id,


            "transaction_id":
                transaction_id,


            "amount":
                amount,


            "currency":
                "USD",


            "payment_method":
                payment_method,


            "status":
                "SUCCESS"


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

        "PAYMENT",

        payment_id,

        correlation_id,

        Json(event)

        )

        )



        conn.commit()


        print(
            "Payment completed:",
            payment_id
        )


        return event



    except Exception as e:

        conn.rollback()

        raise e


    finally:

        cursor.close()

        conn.close()



if __name__=="__main__":

    create_payment_success()
