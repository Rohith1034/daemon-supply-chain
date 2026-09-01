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


EVENT_NAME = "ASNReceived"


def _get_asn_received_time(po, shipment):
    """
    ASN received time must be later than:
        - PO order/approval time
        - shipment creation time
        - current simulation time

    This prevents impossible timelines such as ASN
    happening before shipment creation.
    """
    simulation_now = get_simulation_now()

    candidates = [
        candidate for candidate in [
            po.get("updated_at"),
            po.get("order_date"),
            shipment.get("shipment_date"),
            shipment.get("updated_at"),
            simulation_now
        ] if candidate is not None
    ]

    base_time = max(candidates)

    # Add a realistic inbound processing delay.
    return (
        base_time +
        timedelta(
            minutes=random.randint(30, 360)
        )
    )


def generate_asn_received():

    with Database() as db:

        # ---------------------------------------
        # Find approved PO with created shipment
        # ---------------------------------------

        po = db.fetch_one(
            """
            SELECT
                po.po_id,
                po.supplier_id,
                po.warehouse_id,
                po.correlation_id,
                po.order_date,
                po.updated_at,
                s.shipment_id,
                s.shipment_date,
                s.updated_at AS shipment_updated_at
            FROM purchase_orders po
            INNER JOIN shipments s
                ON po.po_id = s.po_id
            WHERE po.po_status='APPROVED'
              AND s.shipment_status='CREATED'
            ORDER BY po.created_at
            LIMIT 1
            """
        )

        if not po:
            raise Exception(
                "No APPROVED PO with CREATED shipment found"
            )

        po_id = po["po_id"]
        shipment_id = po["shipment_id"]
        supplier_id = po["supplier_id"]
        warehouse_id = po["warehouse_id"]

        correlation_id = str(
            po["correlation_id"]
        )

        # ---------------------------------------
        # ASN received business time
        # ---------------------------------------

        received_at = _get_asn_received_time(
            po,
            po
        )

        # ---------------------------------------
        # Update PO status
        # ---------------------------------------

        db.execute(
            """
            UPDATE purchase_orders
            SET
                po_status=%s,
                updated_at=%s
            WHERE po_id=%s
            """,
            (
                "ASN_RECEIVED",
                received_at,
                po_id
            )
        )

        # ---------------------------------------
        # Update Shipment status
        # ---------------------------------------

        db.execute(
            """
            UPDATE shipments
            SET
                shipment_status=%s,
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                "ASN_RECEIVED",
                received_at,
                shipment_id
            )
        )

        # ---------------------------------------
        # Event Payload
        # ---------------------------------------

        payload = {
            "event_type":
                EVENT_NAME,

            "occurred_at":
                received_at.isoformat(),

            "asn":
            {
                "po_id":
                    po_id,

                "shipment_id":
                    shipment_id,

                "supplier_id":
                    supplier_id,

                "warehouse_id":
                    warehouse_id,

                "status":
                    "RECEIVED"
            },

            "correlation_id":
                correlation_id
        }

        # ---------------------------------------
        # Publish Outbox
        # ---------------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="PURCHASE_ORDER",
            aggregate_id=po_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # ---------------------------------------
        # Log
        # ---------------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "po_id":
                    po_id,

                "shipment_id":
                    shipment_id,

                "supplier_id":
                    supplier_id,

                "warehouse_id":
                    warehouse_id,

                "received_at":
                    received_at,

                "correlation_id":
                    correlation_id
            }
        )

        print(
f"""
============================================================
EVENT : {EVENT_NAME}

PO ID               : {po_id}
SHIPMENT ID         : {shipment_id}
SUPPLIER ID         : {supplier_id}
WAREHOUSE ID        : {warehouse_id}
CORRELATION ID      : {correlation_id}
RECEIVED AT         : {received_at}

STATUS : SUCCESS
============================================================
"""
        )


if __name__ == "__main__":

    try:
        generate_asn_received()

    except Exception as e:
        log_event_failure(
            EVENT_NAME,
            e
        )
        raise