import random
from datetime import timedelta, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure,
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "ShipmentDelivered"


# ==========================================================
# UTC HELPER
# ==========================================================

def _ensure_utc(dt):

    if dt is None:
        return None

    if dt.tzinfo is None:
        return dt.replace(
            tzinfo=timezone.utc
        )

    return dt.astimezone(
        timezone.utc
    )


# ==========================================================
# MAIN EVENT
# ==========================================================

def generate_shipment_delivered(shipment_id=None):

    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

DELIVERING SHIPMENT

============================================================
"""
        )


        # ====================================================
        # FIND IN TRANSIT SHIPMENT
        # ====================================================

        if shipment_id:

            shipment = db.fetch_one(
                """
                SELECT
                    shipment_id,
                    fulfillment_id,
                    order_id,
                    package_id,
                    correlation_id,
                    expected_delivery
                FROM outbound_shipments
                WHERE shipment_id=%s
                AND shipment_status='IN_TRANSIT'
                """,
                (
                    shipment_id,
                )
            )

        else:

            shipment = db.fetch_one(
                """
                SELECT
                    shipment_id,
                    fulfillment_id,
                    order_id,
                    package_id,
                    correlation_id,
                    expected_delivery
                FROM outbound_shipments
                WHERE shipment_status='IN_TRANSIT'
                ORDER BY shipment_date
                LIMIT 1
                """
            )


        if not shipment:

            raise Exception(
                "No IN_TRANSIT shipment found"
            )



        shipment_id = shipment["shipment_id"]

        order_id = shipment["order_id"]

        fulfillment_id = shipment["fulfillment_id"]

        package_id = shipment["package_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )



        print(
            f"""
============================================================
SHIPMENT FOUND

SHIPMENT:
{shipment_id}

ORDER:
{order_id}

PACKAGE:
{package_id}

============================================================
"""
        )



        # ====================================================
        # GET TRANSPORTATION DETAILS
        # ====================================================

        transport = db.fetch_one(
            """
            SELECT
                vehicle_id,
                trailer_id,
                driver_id,
                picked_up_at
            FROM outbound_shipment_transportation
            WHERE shipment_id=%s
            """,
            (
                shipment_id,
            )
        )


        if not transport:

            raise Exception(
                "Transportation record not found"
            )



        pickup_time = _ensure_utc(
            transport["picked_up_at"]
        )


        if pickup_time is None:

            pickup_time = _ensure_utc(
                get_simulation_now()
            )



        delivery_time = (
            pickup_time
            +
            timedelta(
                hours=random.randint(
                    6,
                    72
                )
            )
        )



        # ====================================================
        # UPDATE SHIPMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipments
            SET
                shipment_status='DELIVERED',
                actual_delivery=%s,
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                delivery_time,
                delivery_time,
                shipment_id
            )
        )




        # ====================================================
        # UPDATE TRANSPORTATION
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_transportation
            SET
                status='DELIVERED',
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                delivery_time,
                shipment_id
            )
        )



        # ====================================================
        # UPDATE TRACKING
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_tracking
            SET
                status='DELIVERED',
                actual_arrival=%s
            WHERE shipment_id=%s
            """,
            (
                delivery_time,
                shipment_id
            )
        )



        # ====================================================
        # RELEASE RESOURCES
        # ====================================================

        db.execute(
            """
            UPDATE vehicles
            SET
                status='ACTIVE'
            WHERE vehicle_id=%s
            """,
            (
                transport["vehicle_id"],
            )
        )


        db.execute(
            """
            UPDATE trailers
            SET
                status='ACTIVE'
            WHERE trailer_id=%s
            """,
            (
                transport["trailer_id"],
            )
        )


        db.execute(
            """
            UPDATE drivers
            SET
                status='ACTIVE'
            WHERE driver_id=%s
            """,
            (
                transport["driver_id"],
            )
        )



        # ====================================================
        # COMPLETE FULFILLMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_fulfillment
            SET
                status='COMPLETED',
                completed_at=%s
            WHERE fulfillment_id=%s
            """,
            (
                delivery_time,
                fulfillment_id
            )
        )



        # ====================================================
        # COMPLETE ORDER
        # ====================================================

        db.execute(
            """
            UPDATE orders
            SET
                order_status='DELIVERED'
            WHERE order_id=%s
            """,
            (
                order_id,
            )
        )



        # ====================================================
        # PAYLOAD
        # ====================================================

        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                delivery_time.isoformat(),


            "shipment":
            {

                "shipment_id":
                    shipment_id,

                "package_id":
                    package_id,

                "order_id":
                    order_id,

                "fulfillment_id":
                    fulfillment_id,

                "vehicle_id":
                    transport["vehicle_id"],

                "trailer_id":
                    transport["trailer_id"],

                "driver_id":
                    transport["driver_id"],

                "status":
                    "DELIVERED"

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
            aggregate_type="SHIPMENT",
            aggregate_id=shipment_id,
            correlation_id=correlation_id,
            payload=payload
        )



        log_event_success(
            EVENT_NAME,
            {
                "shipment_id": shipment_id,
                "order_id": order_id,
                "driver_id": transport["driver_id"]
            }
        )



        print(
            f"""
============================================================
SHIPMENT DELIVERED

SHIPMENT:
{shipment_id}

ORDER:
{order_id}

DRIVER:
{transport["driver_id"]}

STATUS:
DELIVERED

============================================================
"""
        )



        return {

            "shipment_id":
                shipment_id,

            "status":
                "DELIVERED"

        }



# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    try:

        generate_shipment_delivered()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise