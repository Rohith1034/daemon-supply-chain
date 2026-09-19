from datetime import timedelta
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.enums import POStatus

from core.simulation_clock import (
    get_simulation_now
)
from psycopg2.extras import register_uuid
register_uuid()


EVENT_NAME = "PurchaseOrderApproved"



def approve_purchase_order():


    with Database() as db:


        # --------------------------------
        # Find CREATED PO
        # --------------------------------

        po = db.fetch_one(
            """
            SELECT *
            FROM purchase_orders
            WHERE po_status=%s
            ORDER BY order_date
            LIMIT 1
            """,
            (
                POStatus.CREATED.value,
            )
        )


        if not po:

            raise Exception(
                "No CREATED purchase order found"
            )



        po_id = po["po_id"]



        # --------------------------------
        # Validate PO items
        # --------------------------------

        item_count = db.fetch_one(
            """
            SELECT COUNT(*) AS count
            FROM purchase_order_items
            WHERE po_id=%s
            """,
            (
                po_id,
            )
        )


        if item_count["count"] == 0:

            raise Exception(
                f"PO {po_id} has no items"
            )



        # --------------------------------
        # Business timestamp
        # --------------------------------


        base_time = max(
            po["order_date"],
            get_simulation_now()
        )


        approved_at = (
            base_time
            +
            timedelta(
                minutes=random.randint(
                    5,
                    180
                )
            )
        )



        # --------------------------------
        # Update status
        # --------------------------------


        db.execute(
            """
            UPDATE purchase_orders
            SET
                po_status=%s,
                updated_at=%s
            WHERE po_id=%s
            """,
            (
                POStatus.APPROVED.value,
                approved_at,
                po_id
            )
        )



        # --------------------------------
        # Fetch items for event
        # --------------------------------


        items = db.fetch_all(
            """
            SELECT
                product_id,
                ordered_quantity,
                unit_cost
            FROM purchase_order_items
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
                        item["product_id"],

                    "quantity":
                        item["ordered_quantity"],

                    "unit_cost":
                        float(
                            item["unit_cost"]
                        )
                }
            )



        # --------------------------------
        # Event payload
        # --------------------------------


        payload = {

            "event_type":
                EVENT_NAME,

            "po_id":
                po_id,

            "supplier_id":
                po["supplier_id"],

            "warehouse_id":
                po["warehouse_id"],

            "status":
                POStatus.APPROVED.value,

            "approved_by":
                "SYSTEM",

            "order_date":
                po["order_date"].isoformat(),

            "approved_at":
                approved_at.isoformat(),

            "items":
                payload_items,

            "correlation_id":
                str(
                    po["correlation_id"]
                )
        }



        # --------------------------------
        # Outbox
        # --------------------------------


        publish_event(
            db=db,

            event_type=EVENT_NAME,

            aggregate_type="PURCHASE_ORDER",

            aggregate_id=po_id,

            correlation_id=
                po["correlation_id"],

            payload=payload
        )



        log_event_success(
            EVENT_NAME,
            {
                "po_id":
                    po_id,

                "supplier_id":
                    po["supplier_id"],

                "warehouse_id":
                    po["warehouse_id"],

                "approved_at":
                    approved_at,

                "items":
                    len(items),

                "correlation_id":
                    str(
                        po["correlation_id"]
                    )
            }
        )



if __name__=="__main__":


    try:

        approve_purchase_order()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise