from datetime import datetime, timezone, timedelta
import uuid
import random

from core.db import Database
from core.ids import next_order_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "OrderCreated"



def generate_order_created():

    with Database() as db:


        # -------------------------------------------------
        # Select customer
        # -------------------------------------------------

        customer = db.fetch_one(
            """
            SELECT customer_id
            FROM customers
            ORDER BY random()
            LIMIT 1
            """
        )


        if not customer:
            raise Exception(
                "No customer found"
            )


        customer_id = customer["customer_id"]



        # -------------------------------------------------
        # Select warehouse having available inventory
        # Generated column FIX
        # -------------------------------------------------

        warehouse = db.fetch_one(
            """
            SELECT warehouse_id
            FROM inventory
            WHERE available_quantity > 0
            GROUP BY warehouse_id
            ORDER BY random()
            LIMIT 1
            """
        )


        if not warehouse:

            raise Exception(
                "No warehouse with available inventory"
            )


        warehouse_id = warehouse["warehouse_id"]



        # -------------------------------------------------
        # Generate order id
        # -------------------------------------------------

        order_id = next_order_id(db)


        now = datetime.now(
            timezone.utc
        )


        promised_delivery = now + timedelta(
            days=random.randint(1,7)
        )


        correlation_id = str(
            uuid.uuid4()
        )



        order_channel = random.choice(
            [
                "ONLINE",
                "MOBILE_APP",
                "MARKETPLACE"
            ]
        )


        priority = random.choice(
            [
                "NORMAL",
                "HIGH",
                "URGENT"
            ]
        )



        # -------------------------------------------------
        # Insert order
        # ONLY columns existing in your schema
        # -------------------------------------------------

        db.execute(
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
                order_date,
                created_at,
                promised_delivery_date,
                correlation_id
            )

            VALUES
            (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (

                order_id,

                customer_id,

                warehouse_id,

                "CREATED",

                order_channel,

                priority,

                0,

                0,

                0,

                "USD",

                now,

                now,

                promised_delivery,

                correlation_id

            )
        )



        # -------------------------------------------------
        # Publish event
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


                "customerId":
                    customer_id,


                "warehouseId":
                    warehouse_id,


                "status":
                    "CREATED",


                "channel":
                    order_channel,


                "priority":
                    priority

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



        log_event_success(

            EVENT_NAME,

            {

                "order_id":
                    order_id,


                "customer_id":
                    customer_id,


                "warehouse_id":
                    warehouse_id,


                "status":
                    "CREATED",


                "correlation_id":
                    correlation_id

            }

        )




if __name__ == "__main__":


    try:

        generate_order_created()


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise