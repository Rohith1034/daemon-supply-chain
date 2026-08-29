from datetime import datetime, timezone, timedelta
import random

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentInTransit"



def generate_shipment_in_transit():

    with Database() as db:

        # ------------------------------------
        # Find picked up shipment
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id
            FROM shipments
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


        correlation_id = str(
            shipment["correlation_id"]
        )



        # ------------------------------------
        # Get transportation details
        # ------------------------------------

        transportation = db.fetch_one(
            """
            SELECT
                vehicle_id,
                trailer_id,
                driver_id
            FROM shipment_transportation
            WHERE shipment_id=%s
            ORDER BY assigned_at DESC
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )


        if not transportation:

            raise Exception(
                "Transportation details not found"
            )



        vehicle_id = transportation["vehicle_id"]

        driver_id = transportation["driver_id"]

        trailer_id = transportation["trailer_id"]



        now = datetime.now(
            timezone.utc
        )


        estimated_arrival = (
            now + timedelta(days=2)
        )



        # ------------------------------------
        # Find PICKED_UP tracking
        # ------------------------------------

        tracking = db.fetch_one(
            """
            SELECT
                tracking_id
            FROM shipment_tracking
            WHERE shipment_id=%s
            AND status='PICKED_UP'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )


        if not tracking:

            raise Exception(
                "No PICKED_UP tracking record found"
            )


        tracking_id = tracking["tracking_id"]



        # ------------------------------------
        # Generate GPS
        # ------------------------------------

        latitude = round(
            random.uniform(
                8.0,
                20.0
            ),
            6
        )


        longitude = round(
            random.uniform(
                72.0,
                85.0
            ),
            6
        )



        # ------------------------------------
        # Update shipment
        # ------------------------------------

        db.execute(

            """
            UPDATE shipments

            SET

                shipment_status='IN_TRANSIT',

                updated_at=%s

            WHERE shipment_id=%s

            """,

            (
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

                status='IN_TRANSIT',

                latitude=%s,

                longitude=%s,

                estimated_arrival=%s

            WHERE tracking_id=%s

            """,

            (

                latitude,

                longitude,

                estimated_arrival,

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


                "vehicleId":
                    vehicle_id,


                "trailerId":
                    trailer_id,


                "driverId":
                    driver_id,


                "status":
                    "IN_TRANSIT",


                "trackingId":
                    tracking_id,


                "latitude":
                    latitude,


                "longitude":
                    longitude,


                "estimatedArrival":
                    estimated_arrival.isoformat()

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


                "vehicle_id":
                    vehicle_id,


                "driver_id":
                    driver_id,


                "status":
                    "IN_TRANSIT",


                "tracking_id":
                    tracking_id,


                "latitude":
                    latitude,


                "longitude":
                    longitude,


                "correlation_id":
                    correlation_id

            }

        )




if __name__ == "__main__":

    try:

        generate_shipment_in_transit()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise