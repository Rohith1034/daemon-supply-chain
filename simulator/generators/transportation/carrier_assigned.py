import random
from datetime import timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "CarrierAssigned"



# ============================================================
# TIME HELPER
# ============================================================

def _ensure_utc(value):

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )



# ============================================================
# MAIN EVENT
# ============================================================

def generate_carrier_assigned(shipment_id=None):



    with Database() as db:



        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

ASSIGNING TRANSPORTATION

============================================================
"""
        )



        # ====================================================
        # FIND READY SHIPMENT
        # ====================================================

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                order_id,
                package_id,
                correlation_id
            FROM outbound_shipments
            WHERE shipment_status='READY'
            ORDER BY shipment_date
            LIMIT 1
            """
        )


        if not shipment:

            raise Exception(
                "No READY shipment found"
            )



        shipment_id = shipment["shipment_id"]

        order_id = shipment["order_id"]

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
        # FIND AVAILABLE VEHICLE
        # ====================================================

        vehicle = db.fetch_one(
            """
            SELECT
                vehicle_id
            FROM vehicles
            WHERE status='ACTIVE'
            ORDER BY created_at
            LIMIT 1
            """
        )


        if not vehicle:

            raise Exception(
                "No available vehicle"
            )


        vehicle_id = vehicle["vehicle_id"]




        # ====================================================
        # FIND AVAILABLE TRAILER
        # ====================================================

        trailer = db.fetch_one(
            """
            SELECT
                trailer_id
            FROM trailers
            WHERE status='ACTIVE'
            ORDER BY created_at
            LIMIT 1
            """
        )


        if not trailer:

            raise Exception(
                "No active trailer available"
            )


        trailer_id = trailer["trailer_id"]




        # ====================================================
        # FIND AVAILABLE DRIVER
        # ====================================================

        driver = db.fetch_one(
            """
            SELECT
                driver_id
            FROM drivers
            WHERE status='ACTIVE'
            ORDER BY created_at
            LIMIT 1
            """
        )


        if not driver:

            raise Exception(
                "No available driver"
            )


        driver_id = driver["driver_id"]



        assigned_time = _ensure_utc(
            get_simulation_now()
        )



        # ====================================================
        # CREATE TRANSPORTATION ASSIGNMENT
        # ====================================================

        db.execute(
            """
            INSERT INTO outbound_shipment_transportation
            (
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                assigned_at,
                status
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s
            )
            """,
            (
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                assigned_time,
                "ASSIGNED"
            )
        )



        # ====================================================
        # UPDATE SHIPMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipments
            SET
                shipment_status='ASSIGNED',
                carrier_service='STANDARD'
            WHERE shipment_id=%s
            """,
            (
                shipment_id,
            )
        )



        # ====================================================
        # UPDATE RESOURCES
        # ====================================================

        db.execute(
            """
            UPDATE vehicles
            SET
                status='ASSIGNED'
            WHERE vehicle_id=%s
            """,
            (
                vehicle_id,
            )
        )


        db.execute(
            """
            UPDATE trailers
            SET
                status='ASSIGNED'
            WHERE trailer_id=%s
            """,
            (
                trailer_id,
            )
        )


        db.execute(
            """
            UPDATE drivers
            SET
                status='ASSIGNED'
            WHERE driver_id=%s
            """,
            (
                driver_id,
            )
        )



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================

        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                assigned_time.isoformat(),


            "transportation":
            {

                "shipment_id":
                    shipment_id,

                "vehicle_id":
                    vehicle_id,

                "trailer_id":
                    trailer_id,

                "driver_id":
                    driver_id,

                "status":
                    "ASSIGNED"

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
                "vehicle_id": vehicle_id,
                "driver_id": driver_id
            }
        )



        print(
            f"""
============================================================
CARRIER ASSIGNED

SHIPMENT:
{shipment_id}

VEHICLE:
{vehicle_id}

TRAILER:
{trailer_id}

DRIVER:
{driver_id}

STATUS:
ASSIGNED

============================================================
"""
        )


        return {

            "shipment_id":
                shipment_id,

            "vehicle_id":
                vehicle_id,

            "trailer_id":
                trailer_id,

            "driver_id":
                driver_id,

            "status":
                "ASSIGNED"

        }





# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_carrier_assigned()


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise