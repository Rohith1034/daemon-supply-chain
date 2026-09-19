from datetime import timedelta
import random

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "OrderItemCreated"



# ============================================================
# TIME HELPERS
# ============================================================

def _get_item_created_time(order):

    simulation_now = get_simulation_now()

    candidates = [
        order.get("order_date"),
        order.get("created_at"),
        simulation_now
    ]

    candidates = [
        x for x in candidates
        if x is not None
    ]

    base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(1,30)
        )
    )



# ============================================================
# FETCH AVAILABLE INVENTORY
# ============================================================

def _fetch_available_inventory(
        db,
        warehouse_id,
        limit
):

    return db.fetch_all(
        """
        SELECT
            i.inventory_id,
            i.product_id,
            i.available_quantity,
            i.location_id,
            p.selling_price
        FROM inventory i
        JOIN products p
            ON i.product_id=p.product_id
        WHERE i.warehouse_id=%s
          AND i.inventory_status='AVAILABLE'
          AND i.available_quantity > 0
          AND i.location_id IS NOT NULL
          AND p.status='ACTIVE'
        ORDER BY random()
        LIMIT %s
        """,
        (
            warehouse_id,
            limit
        )
    )



# ============================================================
# MAIN EVENT
# ============================================================

def generate_order_item_created(
        order_id=None
):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CREATING ORDER ITEMS

============================================================
"""
        )


        # ====================================================
        # FIND ORDER
        # ====================================================


        if order_id:

            order = db.fetch_one(
                """
                SELECT
                    order_id,
                    customer_id,
                    warehouse_id,
                    order_date,
                    created_at,
                    correlation_id
                FROM orders
                WHERE order_id=%s
                  AND order_status='CREATED'
                  AND items_created=false
                LIMIT 1
                """,
                (
                    order_id,
                )
            )


        else:


            order = db.fetch_one(
                """
                SELECT
                    order_id,
                    customer_id,
                    warehouse_id,
                    order_date,
                    created_at,
                    correlation_id
                FROM orders
                WHERE order_status='CREATED'
                  AND items_created=false
                ORDER BY created_at DESC
                LIMIT 1
                """
            )


        if not order:

            raise Exception(
                "No CREATED order waiting for items"
            )



        order_id = order["order_id"]

        warehouse_id = order["warehouse_id"]

        correlation_id = str(
            order["correlation_id"]
        )



        item_created_time = _get_item_created_time(
            order
        )



        print(
            f"""
============================================================
ORDER FOUND

ORDER ID :
{order_id}

WAREHOUSE :
{warehouse_id}

CORRELATION ID :
{correlation_id}

============================================================
"""
        )



        # ====================================================
        # FETCH PRODUCTS FROM INVENTORY
        # ====================================================


        item_count = random.randint(
            1,
            4
        )


        inventory_rows = _fetch_available_inventory(
            db,
            warehouse_id,
            item_count * 3
        )


        if not inventory_rows:

            raise Exception(
                f"No inventory available for order {order_id}"
            )



        items = []

        total_quantity = 0

        total_amount = 0



        # ====================================================
        # CREATE ORDER ITEMS
        # ====================================================


        for row in inventory_rows[:item_count]:


            available_qty = int(
                row["available_quantity"]
            )


            quantity = random.randint(
                1,
                min(
                    available_qty,
                    5
                )
            )


            unit_price = float(
                row["selling_price"]
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
                (
                    %s,%s,%s,%s,%s
                )
                """,
                (
                    order_id,
                    row["product_id"],
                    quantity,
                    unit_price,
                    total_price
                )
            )



            items.append(
                {

                    "product_id":
                        row["product_id"],

                    "inventory_id":
                        row["inventory_id"],

                    "location_id":
                        row["location_id"],

                    "quantity":
                        quantity,

                    "unit_price":
                        unit_price,

                    "total_price":
                        total_price

                }
            )


            total_quantity += quantity

            total_amount += total_price




        if not items:

            raise Exception(
                "No order items created"
            )



        # ====================================================
        # UPDATE ORDER
        # ====================================================


        db.execute(
            """
            UPDATE orders
            SET
                total_items=%s,
                total_quantity=%s,
                total_amount=%s,
                items_created=true
            WHERE order_id=%s
            """,
            (
                len(items),
                total_quantity,
                round(total_amount,2),
                order_id
            )
        )



        print(
            f"""
============================================================
ORDER UPDATED

ORDER ID :
{order_id}

ITEM COUNT :
{len(items)}

TOTAL QTY :
{total_quantity}

TOTAL AMOUNT :
{round(total_amount,2)}

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
                item_created_time.isoformat(),



            "order":

            {

                "order_id":
                    order_id,


                "warehouse_id":
                    warehouse_id,


                "items":
                    items,


                "total_items":
                    len(items),


                "total_quantity":
                    total_quantity,


                "total_amount":
                    round(total_amount,2)

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



        # ====================================================
        # SUCCESS LOG
        # ====================================================


        log_event_success(
            EVENT_NAME,
            {

                "order_id":
                    order_id,

                "items":
                    len(items),

                "quantity":
                    total_quantity,

                "amount":
                    round(total_amount,2)

            }
        )



        print(
            f"""
============================================================
EVENT : {EVENT_NAME}

STATUS : SUCCESS

ORDER ID :
{order_id}

ITEMS CREATED :
{len(items)}

============================================================
"""
        )


        return {

            "order_id":
                order_id,

            "items":
                items

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_order_item_created()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise