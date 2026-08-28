import random

from datetime import datetime, timezone

from psycopg2.extras import execute_values, Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_customer(cursor):

    cursor.execute(
        """
        SELECT customer_id
        FROM customers
        ORDER BY random()
        LIMIT 1
        """
    )

    return cursor.fetchone()



def get_products(cursor):

    cursor.execute(
        """
        SELECT
        product_id,
        selling_price

        FROM products

        ORDER BY random()

        LIMIT %s

        """,
        (
            random.randint(1,5),
        )
    )

    return cursor.fetchall()



def get_warehouse(cursor):

    cursor.execute(
        """
        SELECT warehouse_id

        FROM warehouses

        ORDER BY random()

        LIMIT 1
        """
    )

    return cursor.fetchone()



def create_order():


    conn=get_connection()

    cursor=conn.cursor()


    try:


        customer=get_customer(cursor)


        warehouse=get_warehouse(cursor)


        products=get_products(cursor)



        if not customer or not warehouse or not products:

            raise Exception(
                "Missing master data"
            )



        order_id=(

            f"ORD-{random.randint(1,99999999):08d}"

        )



        now=datetime.now(
            timezone.utc
        )


        items=[]

        total_quantity=0

        total_amount=0



        for product in products:


            qty=random.randint(
                1,
                5
            )


            price=float(
                product[1]
            )


            total=qty*price


            total_quantity += qty

            total_amount += total



            items.append(

            {

            "product_id":
                product[0],

            "quantity":
                qty,

            "unit_price":
                price,

            "total_price":
                total

            }

            )



        #
        # Insert order
        #

        cursor.execute(

        """

        INSERT INTO orders

        (

        order_id,

        customer_id,

        warehouse_id,

        order_status,

        order_channel,

        priority,

        total_items,

        total_quantity,

        total_amount,

        currency,

        order_date

        )


        VALUES

        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

        """,

        (

        order_id,

        customer[0],

        warehouse[0],

        "CREATED",

        random.choice(
            [
            "WEB",
            "MOBILE_APP",
            "MARKETPLACE"
            ]
        ),

        random.choice(
            [
            "NORMAL",
            "HIGH"
            ]
        ),

        len(items),

        total_quantity,

        total_amount,

        "USD",

        now

        )

        )



        #
        # Insert items
        #

        item_rows=[]


        for item in items:

            item_rows.append(

            (

            order_id,

            item["product_id"],

            item["quantity"],

            item["unit_price"],

            item["total_price"]

            )

            )



        execute_values(

        cursor,

        """

        INSERT INTO order_items

        (

        order_id,

        product_id,

        quantity,

        unit_price,

        total_price

        )

        VALUES %s

        """,

        item_rows

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
            "OrderCreated",


        "event_version":
            "1.0",


        "timestamp":
            now.isoformat(),


        "source":
            "order-service",


        "aggregate_type":
            "ORDER",


        "aggregate_id":
            order_id,


        "correlation_id":
            correlation_id,


        "order":{


            "order_id":
                order_id,


            "customer_id":
                customer[0],


            "warehouse_id":
                warehouse[0],


            "items":
                items,


            "total_quantity":
                total_quantity,


            "total_amount":
                total_amount,


            "status":
                "CREATED"

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



        conn.commit()


        print(
            "Created:",
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

    create_order()