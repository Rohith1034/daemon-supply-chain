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


EVENT_NAME = "SupplierShipmentDelivered"


def _get_delivery_time(shipment):
    """
    Delivery must happen after:

    1. Shipment creation
    2. ASN arrival
    3. Expected delivery window
    4. Simulation clock

    """

    simulation_now = get_simulation_now()


    candidates = [

        shipment.get("updated_at"),

        shipment.get("shipment_date"),

        shipment.get("expected_delivery"),

        simulation_now

    ]


    candidates = [
        value
        for value in candidates
        if value is not None
    ]


    base_time = max(candidates)


    return (
        base_time +
        timedelta(
            hours=random.randint(
                1,
                24
            )
        )
    )



def deliver_supplier_shipment():


    with Database() as db:


        # ---------------------------------------
        # Find arrived shipment
        # ---------------------------------------

        shipment = db.fetch_one(
            """
            SELECT

                shipment_id,
                po_id,
                supplier_id,
                warehouse_id,

                shipment_date,
                expected_delivery,
                updated_at,

                correlation_id

            FROM shipments

            WHERE shipment_status='ASN_RECEIVED'

            ORDER BY shipment_date

            LIMIT 1
            """
        )


        if not shipment:

            raise Exception(
                "No ARRIVED shipment found"
            )



        shipment_id = shipment["shipment_id"]

        po_id = shipment["po_id"]

        supplier_id = shipment["supplier_id"]

        warehouse_id = shipment["warehouse_id"]


        correlation_id = str(
            shipment["correlation_id"]
        )


        # ---------------------------------------
        # Validate shipment items
        # ---------------------------------------

        items = db.fetch_one(
            """
            SELECT
                COUNT(*) AS count

            FROM shipment_items

            WHERE shipment_id=%s
            """,
            (
                shipment_id,
            )
        )


        if items["count"] == 0:

            raise Exception(
                f"Shipment {shipment_id} has no items"
            )



        # ---------------------------------------
        # Delivery timestamp
        # ---------------------------------------

        delivered_at = _get_delivery_time(
            shipment
        )



        # ---------------------------------------
        # Update shipment
        # ---------------------------------------

        db.execute(
            """
            UPDATE shipments

            SET

                shipment_status=%s,

                actual_delivery=%s,

                updated_at=%s

            WHERE shipment_id=%s

            """,
            (

                "DELIVERED",

                delivered_at,

                delivered_at,

                shipment_id

            )
        )



        # ---------------------------------------
        # Event Payload
        # ---------------------------------------

        payload = {


            "event_type":
                EVENT_NAME,


            "shipment_id":
                shipment_id,


            "po_id":
                po_id,


            "supplier_id":
                supplier_id,


            "warehouse_id":
                warehouse_id,


            "status":
                "DELIVERED",


            "delivered_at":
                delivered_at.isoformat(),


            "correlation_id":
                correlation_id

        }



        # ---------------------------------------
        # Publish event
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
        # Log
        # ---------------------------------------

        log_event_success(

            EVENT_NAME,

            {

                "shipment_id":
                    shipment_id,


                "po_id":
                    po_id,


                "warehouse_id":
                    warehouse_id,


                "delivered_at":
                    delivered_at,


                "correlation_id":
                    correlation_id

            }
        )


        print(
f"""
============================================================
EVENT : {EVENT_NAME}

SHIPMENT ID      : {shipment_id}
PO ID            : {po_id}
SUPPLIER ID      : {supplier_id}
WAREHOUSE ID     : {warehouse_id}

DELIVERED AT     : {delivered_at}

CORRELATION ID   : {correlation_id}

STATUS : SUCCESS
============================================================
"""
        )



if __name__ == "__main__":


    try:

        deliver_supplier_shipment()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise