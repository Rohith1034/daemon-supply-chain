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


EVENT_NAME = "ShipmentPickedUp"



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

def generate_shipment_picked_up(shipment_id=None):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

SHIPMENT PICKUP STARTED

============================================================
"""
        )


        # ====================================================
        # FIND SHIPMENT
        # ====================================================

        if shipment_id:

            shipment = db.fetch_one(
                """
                SELECT
                    shipment_id,
                    order_id,
                    correlation_id
                FROM outbound_shipments
                WHERE shipment_id=%s
                  AND shipment_status='PICKED_UP'
                LIMIT 1
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
                    order_id,
                    correlation_id
                FROM outbound_shipments
                WHERE shipment_status='PICKED_UP'
                ORDER BY created_at
                LIMIT 1
                """
            )


        if not shipment:

            raise Exception(
                "No PICKED_UP shipment found"
            )



        shipment_id = shipment["shipment_id"]

        order_id = shipment["order_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )



        # ====================================================
        # FIND TRANSPORT DETAILS
        # ====================================================

        transport = db.fetch_one(
            """
            SELECT
                vehicle_id,
                trailer_id,
                driver_id
            FROM outbound_shipment_transportation
            WHERE shipment_id=%s
              AND status='PICKED_UP'
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )


        if not transport:

            raise Exception(
                "Transportation assignment missing"
            )


        vehicle_id = transport["vehicle_id"]

        trailer_id = transport["trailer_id"]

        driver_id = transport["driver_id"]



        event_time = _ensure_utc(
            get_simulation_now()
        )



        eta = (
            event_time
            +
            timedelta(
                days=random.randint(
                    1,
                    5
                )
            )
        )



        # ====================================================
        # UPDATE TRANSPORTATION
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_transportation
            SET
                status='IN_TRANSIT',
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                event_time,
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
                shipment_status='IN_TRANSIT',
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                event_time,
                shipment_id
            )
        )



        # ====================================================
        # CREATE TRACKING EVENT
        # ====================================================

        tracking_id = _generate_tracking_id()


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
                "IN_TRANSIT",
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
                event_time,
                eta,
                correlation_id
            )
        )



        # ====================================================
        # PAYLOAD
        # ====================================================

        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                event_time.isoformat(),


            "shipment":

            {

                "shipment_id":
                    shipment_id,


                "order_id":
                    order_id,


                "vehicle_id":
                    vehicle_id,


                "trailer_id":
                    trailer_id,


                "driver_id":
                    driver_id,


                "tracking_id":
                    tracking_id,


                "status":
                    "IN_TRANSIT"

            },


            "correlation_id":
                correlation_id

        }



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
SHIPMENT PICKED UP

SHIPMENT:
{shipment_id}

VEHICLE:
{vehicle_id}

DRIVER:
{driver_id}

STATUS:
IN_TRANSIT

============================================================
"""
        )


        return {

            "shipment_id":
                shipment_id,

            "status":
                "IN_TRANSIT"

        }




if __name__ == "__main__":

    try:

        generate_shipment_picked_up()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise