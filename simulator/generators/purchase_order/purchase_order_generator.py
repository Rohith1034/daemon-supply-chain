import random
from datetime import datetime, timezone, timedelta

from psycopg2.extras import execute_values, Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)
from services.po_service import generate_po_id


def get_supplier(cursor):
    cursor.execute(
        """
        SELECT *
        FROM suppliers
        ORDER BY random()
        LIMIT 1
        """
    )

    return cursor.fetchone()


def get_warehouse(cursor):
    cursor.execute(
        """
        SELECT *
        FROM warehouses
        ORDER BY random()
        LIMIT 1
        """
    )

    return cursor.fetchone()


def get_supplier_products(
        cursor,
        supplier_id
):
    cursor.execute(
        """
        SELECT
            product_id,
            name,
            cost_price

        FROM products

        WHERE supplier_id=%s

        ORDER BY random()

        LIMIT %s

        """,
        (
            supplier_id,
            random.randint(2, 10)
        )
    )

    return cursor.fetchall()


def create_purchase_order():
    conn = get_connection()

    cursor = conn.cursor()

    try:

        # Generate PO ID

        po_id = generate_po_id(cursor)

        correlation_id = (
            generate_correlation_id(
                po_id,
                prefix="PO"
            )
        )

        supplier = get_supplier(cursor)

        warehouse = get_warehouse(cursor)

        supplier_id = supplier[0]

        warehouse_id = warehouse[0]

        products = get_supplier_products(
            cursor,
            supplier_id
        )

        items = []

        total_quantity = 0

        total_amount = 0

        for product in products:
            quantity = random.randint(
                10,
                200
            )

            amount = (
                    quantity *
                    float(product[2])
            )

            total_quantity += quantity

            total_amount += amount

            items.append(

                {

                    "product_id":
                        product[0],

                    "quantity":
                        quantity,

                    "unit_cost":
                        float(product[2]),

                    "total_cost":
                        amount

                }

            )

        now = datetime.now(
            timezone.utc
        )

        # -------------------------
        # Insert PO
        # -------------------------

        cursor.execute(

            """
    
            INSERT INTO purchase_orders
    
            (
    
            po_id,
            supplier_id,
            warehouse_id,
            po_status,
            order_date,
            expected_delivery,
            total_items,
            total_quantity,
            total_amount,
            currency
    
            )
    
            VALUES
    
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    
            """,

            (

                po_id,

                supplier_id,

                warehouse_id,

                "CREATED",

                now,

                now + timedelta(
                    days=7
                ),

                len(items),

                total_quantity,

                total_amount,

                "USD"

            )

        )

        # -------------------------
        # Insert PO Items
        # -------------------------

        item_rows = []

        for item in items:
            item_rows.append(

                (

                    po_id,

                    item["product_id"],

                    item["quantity"],

                    item["unit_cost"]

                )

            )

        execute_values(

            cursor,

            """

            INSERT INTO purchase_order_items

            (

            po_id,
            product_id,
            ordered_quantity,
            unit_cost

            )

            VALUES %s


            """,

            item_rows

        )

        # -------------------------
        # Create Event
        # -------------------------

        event = {

            "event_id":
                generate_event_id(),

            "event_type":
                "PurchaseOrderCreated",

            "event_version":
                "1.0",

            "timestamp":
                now.isoformat(),

            "source":
                "procurement-simulator",

            "correlation_id":
                correlation_id,

            "aggregate_type":
                "PURCHASE_ORDER",

            "aggregate_id":
                po_id,

            "purchase_order":

                {

                    "po_id":
                        po_id,

                    "supplier_id":
                        supplier_id,

                    "warehouse_id":
                        warehouse_id,

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

        # -------------------------
        # Event Outbox
        # -------------------------

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

                "PURCHASE_ORDER",

                po_id,

                correlation_id,

                Json(event)

            )

        )

        conn.commit()

        print(
            "Created:",
            po_id
        )

        return event



    except Exception as e:

        conn.rollback()

        raise e


    finally:

        cursor.close()

        conn.close()


print(create_purchase_order())

if __name__ == "__main__":
    create_purchase_order()
