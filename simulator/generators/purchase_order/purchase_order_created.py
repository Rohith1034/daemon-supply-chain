from datetime import timedelta
import random
import uuid
from decimal import Decimal

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
    """
    Return a safe positive lead time in days.

    This protects the flow from invalid or missing
    supplier lead time values.
    """
    lead_time = supplier.get("lead_time_days")

    if lead_time is None:
        return 5

    try:
        lead_time = int(lead_time)
    except (TypeError, ValueError):
        return 5

    return max(lead_time, 1)


def generate_purchase_order():

    with Database() as db:

        # --------------------------------
        # Select Supplier
        # --------------------------------

        supplier = get_active_supplier(db)

        # --------------------------------
        # Select Warehouse
        # --------------------------------

        warehouse = get_active_warehouse(db)

        # --------------------------------
        # Select Supplier Products
        # --------------------------------

        products = get_supplier_products(
            db,
            supplier["supplier_id"],
            random.randint(
                MIN_PO_PRODUCTS,
                MAX_PO_PRODUCTS
            )
        )

        if len(products) < MIN_PO_PRODUCTS:
            raise Exception(
                "Supplier has insufficient products"
            )

        # --------------------------------
        # IDs
        # --------------------------------

        po_id = next_purchase_order_id(db)

        # Real UUID for business correlation.
        correlation_id = str(uuid.uuid4())

        # --------------------------------
        # SIMULATION BUSINESS TIME
        #
        # This is the anchor timestamp for
        # the Purchase Order lifecycle.
        # All downstream events must be
        # derived from this and move forward.
        # --------------------------------

        order_date = get_simulation_now()

        # --------------------------------
        # Expected Delivery
        #
        # Expected delivery must always be
        # after the PO creation time.
        # --------------------------------

        supplier_lead_time_days = _get_supplier_lead_time_days(supplier)

        expected_delivery = (
            order_date +
            timedelta(
                days=supplier_lead_time_days
            )
        )

        # --------------------------------
        # Prepare Items
        # --------------------------------

        items = []
        total_quantity = 0
        total_amount = Decimal("0")

        for product in products:

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

        # --------------------------------
        # Insert Purchase Order
        # --------------------------------

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

        # --------------------------------
        # Insert PO Items
        # --------------------------------
        # total_cost is calculated in Python
        # and stored in the payload. Database
        # item totals can still be derived if
        # needed by the schema.
        # --------------------------------

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

        # --------------------------------
        # Fetch PO
        # --------------------------------

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

        # --------------------------------
        # Event Payload
        # --------------------------------

        payload_items = []

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

        # --------------------------------
        # Outbox
        # --------------------------------

        publish_event(
            db=db,
            event_type="PurchaseOrderCreated",
            aggregate_type="PURCHASE_ORDER",
            aggregate_id=po_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # --------------------------------
        # Log
        # --------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "po_id":
                    po_id,

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

                "supplier_lead_time_days":
                    supplier_lead_time_days,

                "correlation_id":
                    correlation_id
            }
        )


if __name__ == "__main__":

    try:
        generate_purchase_order()

    except Exception as e:
        log_event_failure(
            EVENT_NAME,
            e
        )
        raise
