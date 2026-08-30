from datetime import datetime, timezone
import random


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "OrderItemCreated"



def generate_order_item_created():

    with Database() as db:


        # -------------------------------------------------
        # Find latest CREATED order without items
        # -------------------------------------------------

        order = db.fetch_one(
            """

            SELECT

                o.order_id,
                o.customer_id,
                o.warehouse_id,
                o.correlation_id


            FROM orders o


            LEFT JOIN order_items oi

            ON o.order_id = oi.order_id


            WHERE

                o.order_status = 'CREATED'

                AND oi.order_id IS NULL


            ORDER BY

                o.created_at DESC


            LIMIT 1


            """
        )


        if not order:

            raise Exception(
                "No CREATED order found without items"
            )



        order_id = order["order_id"]

        warehouse_id = order["warehouse_id"]

        correlation_id = str(
            order["correlation_id"]
        )



        now = datetime.now(
            timezone.utc
        )



        # -------------------------------------------------
        # Select inventory products
        # -------------------------------------------------

        products = db.fetch_all(
            """

            SELECT

                i.product_id,

                i.available_quantity,

                p.selling_price


            FROM inventory i


            JOIN products p

            ON i.product_id = p.product_id


            WHERE

                i.warehouse_id=%s

                AND i.available_quantity > 0

                AND p.status='ACTIVE'


            ORDER BY random()


            LIMIT %s


            """,

            (

                warehouse_id,

                random.randint(1,5)

            )

        )



        if not products:

            raise Exception(
                "No inventory available for order"
            )



        total_quantity = 0

        total_amount = 0

        items = []



        # -------------------------------------------------
        # Create order items
        # -------------------------------------------------

        for product in products:


            quantity = random.randint(

                1,

                min(

                    10,

                    product["available_quantity"]

                )

            )


            unit_price = float(

                product["selling_price"]

            )


            total_price = round(

                quantity * unit_price,

                2

            )



            db.execute(

                """

                INSERT INTO order_items

                (

                    order_id,

                    product_id,

                    quantity,

                    unit_price,

                    total_price

                )


                VALUES

                (%s,%s,%s,%s,%s)


                """,

                (

                    order_id,

                    product["product_id"],

                    quantity,

                    unit_price,

                    total_price

                )

            )



            total_quantity += quantity

            total_amount += total_price



            items.append(

                {

                    "productId":

                        product["product_id"],


                    "quantity":

                        quantity,


                    "unitPrice":

                        unit_price,


                    "totalPrice":

                        total_price

                }

            )



        # -------------------------------------------------
        # Update order totals
        # Removed updated_at because orders table does not have it
        # -------------------------------------------------

        db.execute(

            """

            UPDATE orders


            SET

                total_items=%s,

                total_quantity=%s,

                total_amount=%s


            WHERE

                order_id=%s


            """,

            (

                len(items),

                total_quantity,

                round(total_amount,2),

                order_id

            )

        )

        db.execute(
            """
            UPDATE orders
            SET
                order_status='CONFIRMED',
                confirmed_at=%s
            WHERE order_id=%s
            """,
            (
                now,
                order_id
            )
        )

        # -------------------------------------------------
        # Publish Event
        # -------------------------------------------------

        payload = {


            "eventType":

                EVENT_NAME,


            "occurredAt":

                now.isoformat(),


            "order":

            {


                "orderId":

                    order_id,


                "warehouseId":

                    warehouse_id,


                "items":

                    items


            },


            "correlationId":

                correlation_id

        }



        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="ORDER",

            aggregate_id=order_id,

            correlation_id=correlation_id,

            payload=payload

        )



        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        log_event_success(

            EVENT_NAME,

            {


                "order_id":

                    order_id,


                "warehouse_id":

                    warehouse_id,


                "items":

                    len(items),


                "quantity":

                    total_quantity,


                "amount":

                    round(total_amount,2),


                "correlation_id":

                    correlation_id

            }

        )





if __name__ == "__main__":


    try:

        generate_order_item_created()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise