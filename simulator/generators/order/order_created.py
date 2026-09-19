import uuid
import random

from datetime import timezone

from core.db import Database
from core.logger import (
    log_event_failure,
    log_event_success
)
from core.outbox import publish_event
from core.simulation_clock import get_simulation_now


EVENT_NAME = "OrderCreated"


# ============================================================
# DATETIME NORMALIZER
# ============================================================

def _ensure_utc(value):

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )



# ============================================================
# ORDER ID GENERATOR
# ============================================================

def _generate_order_id():

    return (
        "ORD-"
        +
        get_simulation_now()
        .strftime("%Y%m%d")
        +
        "-"
        +
        str(uuid.uuid4())[:8].upper()
    )



# ============================================================
# MAIN EVENT
# ============================================================

def generate_order_created():


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CREATING CUSTOMER ORDER
============================================================
"""
        )


        # ====================================================
        # CORRELATION ID
        # ====================================================

        correlation_id = str(
            uuid.uuid4()
        )


        simulation_now = _ensure_utc(
            get_simulation_now()
        )



        # ====================================================
        # SELECT CUSTOMER
        # ====================================================

        customer = db.fetch_one(
            """
            SELECT
                customer_id,
                first_name,
                last_name,
                city
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



        print(
            f"""
============================================================
CUSTOMER SELECTED

CUSTOMER ID :
{customer_id}

NAME :
{customer["first_name"]}
{customer["last_name"]}

CITY :
{customer["city"]}

============================================================
"""
        )



        # ====================================================
        # SELECT WAREHOUSE
        #
        # IMPORTANT:
        # Order creation does not check inventory
        #
        # ====================================================


        warehouse = db.fetch_one(
            """
            SELECT
                warehouse_id,
                warehouse_name,
                city
            FROM warehouses
            WHERE warehouse_id IN
            (
                SELECT DISTINCT warehouse_id
                FROM inventory
                WHERE inventory_status='AVAILABLE'
                  AND available_quantity > 0
            )
            ORDER BY random()
            LIMIT 1
            """
        )


        if not warehouse:

            raise Exception(
                "No warehouse found"
            )



        warehouse_id = warehouse["warehouse_id"]



        print(
            f"""
============================================================
WAREHOUSE SELECTED

WAREHOUSE ID :
{warehouse_id}

NAME :
{warehouse["warehouse_name"]}

CITY :
{warehouse["city"]}

============================================================
"""
        )



        # ====================================================
        # CREATE ORDER
        # ====================================================

        order_id = _generate_order_id()



        order_channel = random.choice(
            [
                "WEB",
                "MOBILE_APP",
                "MARKETPLACE"
            ]
        )


        priority = random.choice(
            [
                "NORMAL",
                "HIGH"
            ]
        )



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
                correlation_id,
                items_created
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s
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
                simulation_now,
                correlation_id,
                False
            )
        )



        print(
            f"""
============================================================
ORDER CREATED

ORDER ID :
{order_id}

STATUS :
CREATED

CHANNEL :
{order_channel}

PRIORITY :
{priority}

CORRELATION ID :
{correlation_id}

============================================================
"""
        )



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================

        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                simulation_now.isoformat(),



            "order":
            {

                "order_id":
                    order_id,


                "customer_id":
                    customer_id,


                "warehouse_id":
                    warehouse_id,


                "status":
                    "CREATED",


                "channel":
                    order_channel,


                "priority":
                    priority,


                "items_created":
                    False

            },


            "correlation_id":
                correlation_id

        }



        # ====================================================
        # OUTBOX
        # ====================================================

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
                "order_id": order_id,
                "customer_id": customer_id,
                "warehouse_id": warehouse_id,
                "correlation_id": correlation_id
            }
        )



        print(
            f"""
============================================================
EVENT : {EVENT_NAME}

STATUS : SUCCESS

ORDER ID :
{order_id}

CUSTOMER :
{customer_id}

WAREHOUSE :
{warehouse_id}

============================================================
"""
        )



        return {

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




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    try:

        generate_order_created()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise