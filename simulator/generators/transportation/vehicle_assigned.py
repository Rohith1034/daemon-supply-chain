import random
import string
from datetime import timedelta, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "VehicleAssigned"



# ============================================================
# HELPERS
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



def _generate_tracking_id():

    suffix = ''.join(
        random.choices(
            string.ascii_uppercase +
            string.digits,
            k=6
        )
    )

    return f"TRK-{suffix}"



# ============================================================
# MAIN EVENT
# ============================================================

def generate_vehicle_assigned():


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CONFIRMING VEHICLE ASSIGNMENT

============================================================
"""
        )


        # ====================================================
        # FIND ASSIGNED TRANSPORTATION
        # ====================================================

        transport = db.fetch_one(
            """
            SELECT
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id
            FROM outbound_shipment_transportation
            WHERE status='ASSIGNED'
            ORDER BY created_at
            LIMIT 1
            """
        )


        if not transport:

            raise Exception(
                "No ASSIGNED transportation found"
            )


        shipment_id = transport["shipment_id"]

        vehicle_id = transport["vehicle_id"]

        trailer_id = transport["trailer_id"]

        driver_id = transport["driver_id"]



        # ====================================================
        # FIND SHIPMENT
        # ====================================================

        shipment = db.fetch_one(
            """
            SELECT
                order_id,
                correlation_id
            FROM outbound_shipments
            WHERE shipment_id=%s
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )


        if not shipment:

            raise Exception(
                f"Shipment not found {shipment_id}"
            )


        order_id = shipment["order_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )



        assigned_time = _ensure_utc(
            get_simulation_now()
        )



        # ====================================================
        # CREATE INITIAL TRACKING RECORD
        # ====================================================

        tracking_id = _generate_tracking_id()


        estimated_arrival = (
            assigned_time
            +
            timedelta(
                days=random.randint(
                    1,
                    5
                )
            )
        )



        db.execute(
            """
            INSERT INTO outbound_shipment_tracking
            (
                tracking_id,
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                status,
                latitude,
                longitude,
                departure_time,
                estimated_arrival,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s
            )
            """,
            (
                tracking_id,
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                "PICKED_UP",
                round(
                    random.uniform(
                        25,
                        45
                    ),
                    6
                ),
                round(
                    random.uniform(
                        -120,
                        -70
                    ),
                    6
                ),
                assigned_time,
                estimated_arrival,
                correlation_id
            )
        )



        # ====================================================
        # UPDATE TRANSPORTATION
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_transportation
            SET
                status='PICKED_UP',
                picked_up_at=%s,
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                assigned_time,
                assigned_time,
                shipment_id
            )
        )



        # ====================================================
        # UPDATE SHIPMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipments
            SET
                shipment_status='PICKED_UP'
            WHERE shipment_id=%s
            """,
            (
                shipment_id,
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


            "vehicle_assignment":
            {

                "shipment_id":
                    shipment_id,


                "vehicle_id":
                    vehicle_id,


                "trailer_id":
                    trailer_id,


                "driver_id":
                    driver_id,


                "tracking_id":
                    tracking_id,


                "status":
                    "PICKED_UP"

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
                "tracking_id": tracking_id
            }
        )


        print(
            f"""
============================================================
VEHICLE ASSIGNED

SHIPMENT:
{shipment_id}

VEHICLE:
{vehicle_id}

TRAILER:
{trailer_id}

DRIVER:
{driver_id}

TRACKING:
{tracking_id}

STATUS:
PICKED_UP

============================================================
"""
        )


        return {

            "shipment_id":
                shipment_id,

            "tracking_id":
                tracking_id,

            "status":
                "PICKED_UP"

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_vehicle_assigned()


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise