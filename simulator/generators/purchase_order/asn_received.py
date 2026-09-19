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
    ASN timestamp must happen after:

    1. PO approval
    2. Shipment creation
    3. Simulation clock

    ASN belongs to shipment lifecycle.
    """

    simulation_now = get_simulation_now()

    candidates = [
        po.get("updated_at"),
        po.get("order_date"),
        shipment.get("shipment_date"),
        shipment.get("updated_at"),
        simulation_now
    ]

    candidates = [
        value
        for value in candidates
        if value is not None
    ]

    if not candidates:
        return simulation_now

    base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                30,
                360
            )
        )
    )


def generate_asn_received():

    with Database() as db:

        # ---------------------------------------
        # Find shipment waiting for ASN
        # ---------------------------------------

        record = db.fetch_one(
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
                s.updated_at AS shipment_updated_at,
                s.shipment_status

            FROM purchase_orders po

            INNER JOIN shipments s
                ON po.po_id=s.po_id

            WHERE
                s.shipment_status='CREATED'

            ORDER BY
                s.created_at

            LIMIT 1
            """
        )


        if not record:

            raise Exception(
                "No shipment available for ASN"
            )


        shipment = {

            "shipment_id":
                record["shipment_id"],

            "shipment_date":
                record["shipment_date"],

            "updated_at":
                record["shipment_updated_at"]
        }


        po = {

            "po_id":
                record["po_id"],

            "supplier_id":
                record["supplier_id"],

            "warehouse_id":
                record["warehouse_id"],

            "order_date":
                record["order_date"],

            "updated_at":
                record["updated_at"]
        }


        shipment_id = shipment["shipment_id"]

        po_id = po["po_id"]

        correlation_id = str(
            record["correlation_id"]
        )


        # ---------------------------------------
        # ASN business timestamp
        # ---------------------------------------

        received_at = _get_asn_received_time(
            po,
            shipment
        )


        # ---------------------------------------
        # Update shipment state
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
        # Event payload
        # ---------------------------------------

        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                received_at.isoformat(),


            "asn":

            {

                "shipment_id":
                    shipment_id,


                "po_id":
                    po_id,


                "supplier_id":
                    po["supplier_id"],


                "warehouse_id":
                    po["warehouse_id"],


                "status":
                    "ASN_RECEIVED"

            },


            "correlation_id":
                correlation_id

        }



        # ---------------------------------------
        # Publish Event
        # ---------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=correlation_id,

            payload=payload
        )



        # ---------------------------------------
        # Logging
        # ---------------------------------------

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


                "asn_received_at":
                    received_at,


                "correlation_id":
                    correlation_id

            }
        )


        print(
f"""
============================================================
EVENT : {EVENT_NAME}

SHIPMENT ID     : {shipment_id}
PO ID            : {po_id}
SUPPLIER ID      : {po["supplier_id"]}
WAREHOUSE ID     : {po["warehouse_id"]}
CORRELATION ID   : {correlation_id}
ASN RECEIVED AT  : {received_at}

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