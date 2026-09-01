from datetime import timedelta
import random

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "PurchaseOrderApproved"


def approve_purchase_order():

    with Database() as db:

        # --------------------------------
        # Fetch latest CREATED PO
        # --------------------------------

        po = db.fetch_one(
            """
            SELECT *
            FROM purchase_orders
            WHERE po_status='CREATED'
            ORDER BY created_at
            LIMIT 1
            """
        )

        if not po:
            raise Exception(
                "No CREATED purchase order found"
            )

        po_id = po["po_id"]
        previous_status = po["po_status"]

        # --------------------------------
        # SIMULATION / BUSINESS TIME
        #
        # Approval must never be earlier than
        # PO creation. We anchor it to the later
        # of:
        #   1. the PO order_date
        #   2. the current simulation time
        #
        # Then we add a small processing delay.
        # --------------------------------

        order_date = po["order_date"]
        simulation_now = get_simulation_now()

        base_time = max(
            order_date,
            simulation_now
        )

        approved_at = (
            base_time +
            timedelta(
                minutes=random.randint(5, 180)
            )
        )

        # --------------------------------
        # Update PO
        # --------------------------------

        db.execute(
            """
            UPDATE purchase_orders
            SET
                po_status='APPROVED',
                updated_at=%s
            WHERE po_id=%s
            """,
            (
                approved_at,
                po_id
            )
        )

        # --------------------------------
        # Prepare payload
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

            "previous_status":
                previous_status,

            "new_status":
                "APPROVED",

            "order_date":
                order_date.isoformat(),

            "approved_at":
                approved_at.isoformat(),

            "correlation_id":
                str(
                    po["correlation_id"]
                )
        }

        # --------------------------------
        # Outbox Event
        # --------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="PURCHASE_ORDER",
            aggregate_id=po_id,
            correlation_id=str(
                po["correlation_id"]
            ),
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
                    po["supplier_id"],

                "warehouse_id":
                    po["warehouse_id"],

                "order_date":
                    order_date,

                "approved_at":
                    approved_at,

                "correlation_id":
                    str(
                        po["correlation_id"]
                    )
            }
        )


if __name__ == "__main__":

    try:
        approve_purchase_order()

    except Exception as e:
        log_event_failure(
            EVENT_NAME,
            e
        )
        raise
