from datetime import datetime, timezone

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentDelivered"



def generate_shipment_delivered():

    with Database() as db:


        # ------------------------------------
        # Find IN_TRANSIT shipment
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id
            FROM shipments
            WHERE shipment_status='IN_TRANSIT'
            ORDER BY created_at
            LIMIT 1
            """
        )


        if not shipment:

            raise Exception(
                "No IN_TRANSIT shipment found"
            )



        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )


        now = datetime.now(
            timezone.utc
        )



        # ------------------------------------
        # Get transportation details
        # ------------------------------------

        transportation = db.fetch_one(
            """
            SELECT
                vehicle_id,
                driver_id,
                trailer_id
            FROM shipment_transportation
            WHERE shipment_id=%s
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )


        if not transportation:

            raise Exception(
                "Transportation record not found"
            )



        vehicle_id = transportation["vehicle_id"]

        driver_id = transportation["driver_id"]

        trailer_id = transportation["trailer_id"]



        # ------------------------------------
        # Find tracking
        # ------------------------------------

        tracking = db.fetch_one(
            """
            SELECT
                tracking_id
            FROM shipment_tracking
            WHERE shipment_id=%s
            AND status='IN_TRANSIT'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )


        if not tracking:

            raise Exception(
                "IN_TRANSIT tracking not found"
            )


        tracking_id = tracking["tracking_id"]



        # ------------------------------------
        # Update shipment
        # ------------------------------------

        db.execute(
            """
            UPDATE shipments

            SET

                shipment_status='DELIVERED',

                actual_delivery=%s,

                updated_at=%s

            WHERE shipment_id=%s
            """,
            (
                now,
                now,
                shipment_id
            )
        )



        # ------------------------------------
        # Update tracking
        # ------------------------------------

        db.execute(
            """
            UPDATE shipment_tracking

            SET

                status='DELIVERED',

                actual_arrival=%s

            WHERE tracking_id=%s
            """,
            (
                now,
                tracking_id
            )
        )



        # ------------------------------------
        # Event payload
        # ------------------------------------

        payload = {


            "eventType":
                EVENT_NAME,


            "occurredAt":
                now.isoformat(),


            "shipment":

            {

                "shipmentId":
                    shipment_id,


                "warehouseId":
                    warehouse_id,


                "vehicleId":
                    vehicle_id,


                "driverId":
                    driver_id,


                "trailerId":
                    trailer_id,


                "trackingId":
                    tracking_id,


                "status":
                    "DELIVERED",


                "deliveredAt":
                    now.isoformat()

            },


            "correlationId":
                correlation_id

        }



        # ------------------------------------
        # Outbox
        # ------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=correlation_id,

            payload=payload

        )



        # ------------------------------------
        # Output
        # ------------------------------------

        log_event_success(

            EVENT_NAME,

            {

                "shipment_id":
                    shipment_id,


                "warehouse_id":
                    warehouse_id,


                "vehicle_id":
                    vehicle_id,


                "driver_id":
                    driver_id,


                "tracking_id":
                    tracking_id,


                "status":
                    "DELIVERED",


                "correlation_id":
                    correlation_id

            }

        )




if __name__ == "__main__":


    try:

        generate_shipment_delivered()


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise