from datetime import timedelta
import random

from core.db import Database
from core.ids import (
    next_shipment_id
)
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "SupplierShipmentCreated"


def _get_shipment_base_time(po):
    """
    Shipment creation time must never be earlier than
    the PO approval lifecycle.

    Priority order:
        1. PO updated_at (approval time)
        2. PO order_date
        3. current simulation time
    """
    simulation_now = get_simulation_now()

    po_order_date = po.get("order_date")
    po_updated_at = po.get("updated_at")

    candidates = [
        candidate for candidate in [
            po_order_date,
            po_updated_at,
            simulation_now
        ] if candidate is not None
    ]

    if not candidates:
        return simulation_now

    return max(candidates)


def create_supplier_shipment(count=1):

    if count is None or count < 1:
        return []

    created_shipments = []

    for _ in range(count):

        with Database() as db:

            # -------------------------------------------------
            # Find APPROVED PO without existing shipment
            # -------------------------------------------------

            po = db.fetch_one(
                """
                SELECT po.*
                FROM purchase_orders po
                LEFT JOIN shipments s
                    ON po.po_id = s.po_id
                WHERE po.po_status='APPROVED'
                  AND s.po_id IS NULL
                ORDER BY po.created_at
                LIMIT 1
                """
            )

            if not po:
                raise Exception(
                    "No approved PO available without shipment"
                )

            po_id = po["po_id"]

            # -------------------------------------------------
            # Generate shipment ID
            # -------------------------------------------------

            shipment_id = next_shipment_id(db)

            # -------------------------------------------------
            # Shipment business time
            #
            # This must be based on the PO approval
            # timeline, never earlier than the PO itself.
            # -------------------------------------------------

            shipment_date = _get_shipment_base_time(po)

            # Add a realistic outbound supplier preparation delay.
            shipment_date = (
                shipment_date +
                timedelta(
                    hours=random.randint(2, 24)
                )
            )

            # Expected delivery must always be after shipment date.
            expected_delivery = (
                shipment_date +
                timedelta(
                    days=random.randint(2, 7)
                )
            )

            # -------------------------------------------------
            # Fetch PO items
            # -------------------------------------------------

            po_items = db.fetch_all(
                """
                SELECT *
                FROM purchase_order_items
                WHERE po_id=%s
                """,
                (
                    po_id,
                )
            )

            if not po_items:
                raise Exception(
                    "PO has no items"
                )

            total_quantity = sum(
                item["ordered_quantity"]
                for item in po_items
            )

            total_skus = len(po_items)

            # -------------------------------------------------
            # Insert shipment
            # -------------------------------------------------

            db.execute(
                """
                INSERT INTO shipments
                (
                    shipment_id,
                    po_id,
                    supplier_id,
                    warehouse_id,
                    shipment_status,
                    shipment_date,
                    expected_delivery,
                    total_skus,
                    total_quantity,
                    correlation_id
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s
                )
                """,
                (
                    shipment_id,
                    po_id,
                    po["supplier_id"],
                    po["warehouse_id"],
                    "CREATED",
                    shipment_date,
                    expected_delivery,
                    total_skus,
                    total_quantity,
                    po["correlation_id"]
                )
            )

            # -------------------------------------------------
            # Insert shipment items
            # -------------------------------------------------

            for item in po_items:

                db.execute(
                    """
                    INSERT INTO shipment_items
                    (
                        shipment_id,
                        product_id,
                        shipped_quantity
                    )
                    VALUES
                    (%s,%s,%s)
                    """,
                    (
                        shipment_id,
                        item["product_id"],
                        item["ordered_quantity"]
                    )
                )

            # -------------------------------------------------
            # Event payload
            # -------------------------------------------------

            payload = {
                "event_type":
                    EVENT_NAME,

                "shipment_id":
                    shipment_id,

                "po_id":
                    po_id,

                "supplier_id":
                    po["supplier_id"],

                "warehouse_id":
                    po["warehouse_id"],

                "shipment_status":
                    "CREATED",

                "shipment_date":
                    shipment_date.isoformat(),

                "expected_delivery":
                    expected_delivery.isoformat(),

                "total_skus":
                    total_skus,

                "total_quantity":
                    total_quantity,

                "correlation_id":
                    str(
                        po["correlation_id"]
                    )
            }

            # -------------------------------------------------
            # Outbox
            # -------------------------------------------------

            publish_event(
                db=db,
                event_type=EVENT_NAME,
                aggregate_type="SHIPMENT",
                aggregate_id=shipment_id,
                correlation_id=str(
                    po["correlation_id"]
                ),
                payload=payload
            )

            # -------------------------------------------------
            # Logging
            # -------------------------------------------------

            log_event_success(
                EVENT_NAME,
                {
                    "shipment_id":
                        shipment_id,

                    "po_id":
                        po_id,

                    "supplier_id":
                        po["supplier_id"],

                    "warehouse_id":
                        po["warehouse_id"],

                    "shipment_date":
                        shipment_date,

                    "expected_delivery":
                        expected_delivery,

                    "quantity":
                        total_quantity,

                    "correlation_id":
                        str(
                            po["correlation_id"]
                        )
                }
            )

            created_shipments.append(
                {
                    "shipment_id":
                        shipment_id,

                    "po_id":
                        po_id
                }
            )

    return created_shipments


if __name__ == "__main__":

    try:
        result = create_supplier_shipment(
            count=1
        )
        print(result)

    except Exception as e:
        log_event_failure(
            EVENT_NAME,
            e
        )
        raise
