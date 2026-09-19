from datetime import timedelta
import random
import uuid
from decimal import Decimal
from psycopg2.extras import register_uuid
register_uuid()

from core.db import Database

from core.ids import (
    next_purchase_order_id
)

from core.selectors import (
    get_active_supplier,
    get_active_warehouse,
    get_supplier_products
)

from core.enums import POStatus

from core.payloads import (
    build_purchase_order_created_payload
)

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.config import (
    MIN_PO_PRODUCTS,
    MAX_PO_PRODUCTS,
    MIN_PO_QUANTITY,
    MAX_PO_QUANTITY
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "PurchaseOrderCreated"



def _get_supplier_lead_time_days(supplier):

    lead_time = supplier.get(
        "lead_time_days"
    )

    if not lead_time:
        return 5

    try:
        return max(
            int(lead_time),
            1
        )

    except Exception:
        return 5



def generate_purchase_order():


    with Database() as db:


        # -----------------------------
        # Supplier
        # -----------------------------

        supplier = get_active_supplier(db)


        if not supplier:

            raise Exception(
                "No active supplier available"
            )



        # -----------------------------
        # Warehouse
        # -----------------------------

        warehouse = get_active_warehouse(db)


        if not warehouse:

            raise Exception(
                "No active warehouse available"
            )



        # -----------------------------
        # Supplier products
        # -----------------------------

        requested_products = random.randint(
            MIN_PO_PRODUCTS,
            MAX_PO_PRODUCTS
        )


        products = get_supplier_products(
            db,
            supplier["supplier_id"],
            requested_products
        )


        if not products:

            raise Exception(
                f"No products mapped for supplier "
                f"{supplier['supplier_id']}"
            )


        # Do not fail if supplier has fewer products

        products = products[
            :
            requested_products
        ]



        # -----------------------------
        # IDs
        # -----------------------------

        po_id = next_purchase_order_id(db)


        correlation_id = str(uuid.uuid4())



        # -----------------------------
        # Simulation time
        # -----------------------------

        order_date = get_simulation_now()



        lead_days = _get_supplier_lead_time_days(
            supplier
        )


        expected_delivery = (
            order_date
            +
            timedelta(
                days=lead_days
            )
        )



        # -----------------------------
        # Prepare items
        # -----------------------------

        items = []

        total_quantity = 0

        total_amount = Decimal("0")



        for product in products:


            if product["cost_price"] is None:

                continue



            quantity = random.randint(
                MIN_PO_QUANTITY,
                MAX_PO_QUANTITY
            )


            unit_cost = Decimal(
                str(
                    product["cost_price"]
                )
            )


            total_cost = (
                quantity *
                unit_cost
            )


            total_quantity += quantity

            total_amount += total_cost



            items.append(
                {
                    "product": product,
                    "quantity": quantity,
                    "unit_cost": unit_cost,
                    "total_cost": total_cost
                }
            )



        if not items:

            raise Exception(
                "No valid products available"
            )



        # -----------------------------
        # Insert PO
        # -----------------------------


        db.execute(
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
                currency,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s
            )
            """,
            (
                po_id,
                supplier["supplier_id"],
                warehouse["warehouse_id"],
                POStatus.CREATED.value,
                order_date,
                expected_delivery,
                len(items),
                total_quantity,
                total_amount,
                "USD",
                correlation_id
            )
        )



        # -----------------------------
        # Insert PO Items
        # -----------------------------


        for item in items:


            db.execute(
                """
                INSERT INTO purchase_order_items
                (
                    po_id,
                    product_id,
                    ordered_quantity,
                    unit_cost
                )
                VALUES
                (
                    %s,%s,%s,%s
                )
                """,
                (
                    po_id,
                    item["product"]["product_id"],
                    item["quantity"],
                    item["unit_cost"]
                )
            )



        # -----------------------------
        # Fetch PO
        # -----------------------------


        po = db.fetch_one(
            """
            SELECT *
            FROM purchase_orders
            WHERE po_id=%s
            """,
            (
                po_id,
            )
        )



        payload_items=[]


        for item in items:

            payload_items.append(
                {
                    "product_id":
                        item["product"]["product_id"],

                    "name":
                        item["product"]["name"],

                    "ordered_quantity":
                        item["quantity"],

                    "unit_cost":
                        float(
                            item["unit_cost"]
                        ),

                    "total_cost":
                        float(
                            item["total_cost"]
                        )
                }
            )



        payload = build_purchase_order_created_payload(
            po,
            supplier,
            warehouse,
            payload_items
        )



        # -----------------------------
        # Outbox
        # -----------------------------


        publish_event(
            db=db,

            event_type=EVENT_NAME,

            aggregate_type="PURCHASE_ORDER",

            aggregate_id=po_id,

            correlation_id=correlation_id,

            payload=payload
        )



        log_event_success(
            EVENT_NAME,
            {
                "po_id": po_id,

                "supplier_id":
                    supplier["supplier_id"],

                "warehouse_id":
                    warehouse["warehouse_id"],

                "items":
                    len(items),

                "quantity":
                    total_quantity,

                "amount":
                    total_amount,

                "order_date":
                    order_date,

                "expected_delivery":
                    expected_delivery,

                "correlation_id":
                    str(correlation_id)
            }
        )

        return {

            "po_id":
                po_id,

            "supplier_id":
                supplier["supplier_id"],

            "warehouse_id":
                warehouse["warehouse_id"],

            "correlation_id":
                correlation_id,

            "items":
                len(items),

            "quantity":
                total_quantity,

            "amount":
                total_amount

        }




if __name__=="__main__":

    try:

        generate_purchase_order()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise