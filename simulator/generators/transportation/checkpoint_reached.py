from datetime import datetime, timezone
from decimal import Decimal
import random
import uuid


from core.db import Database

from core.ids import (
    next_checkpoint_id
)

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "CheckpointReached"


LOCATIONS = [
    "Warehouse Exit Gate",
    "Highway Entry",
    "Rest Stop",
    "City Border",
    "Distribution Hub",
    "Customer Region",
    "Final Delivery Route"
]


TRAFFIC_STATUS = [
    "NORMAL",
    "MODERATE",
    "HEAVY"
]


WEATHER_STATUS = [
    "CLEAR",
    "RAIN",
    "CLOUDY",
    "FOG"
]


def generate_checkpoint():


    with Database() as db:


        # ------------------------------------------------
        # Get active shipment tracking
        # ------------------------------------------------

        tracking = db.fetch_one(
            """
            SELECT
                st.tracking_id,
                st.shipment_id,
                st.vehicle_id,
                st.driver_id,
                st.correlation_id
            FROM shipment_tracking st
            WHERE st.status='IN_TRANSIT'
            ORDER BY st.created_at
            LIMIT 1
            """
        )


        if not tracking:

            raise Exception(
                "No IN_TRANSIT shipment found"
            )



        shipment_id = tracking["shipment_id"]



        # ------------------------------------------------
        # Determine checkpoint number
        # ------------------------------------------------

        last_checkpoint = db.fetch_one(

            """
            SELECT
                MAX(checkpoint_sequence) AS seq
            FROM shipment_checkpoints
            WHERE shipment_id=%s
            """,

            (
                shipment_id,
            )
        )



        checkpoint_sequence = (

            (last_checkpoint["seq"] or 0)

            +

            1

        )



        # ------------------------------------------------
        # Generate location data
        # ------------------------------------------------


        latitude = Decimal(

            str(
                round(
                    random.uniform(
                        25.0,
                        45.0
                    ),
                    6
                )
            )

        )


        longitude = Decimal(

            str(
                round(
                    random.uniform(
                        -120.0,
                        -70.0
                    ),
                    6
                )
            )

        )


        distance = Decimal(

            str(
                round(
                    random.uniform(
                        20,
                        150
                    ),
                    2
                )
            )

        )


        delay = random.choice(
            [
                0,
                0,
                5,
                15,
                30
            ]
        )


        traffic = random.choice(
            TRAFFIC_STATUS
        )


        weather = random.choice(
            WEATHER_STATUS
        )


        location = random.choice(
            LOCATIONS
        )



        checkpoint_id = next_checkpoint_id(db)



        now = datetime.now(
            timezone.utc
        )



        # ------------------------------------------------
        # Insert checkpoint
        # ------------------------------------------------


        db.execute(

            """

            INSERT INTO shipment_checkpoints

            (

                checkpoint_id,

                shipment_id,

                tracking_id,

                vehicle_id,

                driver_id,

                checkpoint_sequence,

                latitude,

                longitude,

                location_name,

                distance_completed_km,

                estimated_delay_minutes,

                traffic_status,

                weather_condition,

                checkpoint_status,

                checkpoint_time,

                correlation_id

            )


            VALUES

            (

                %s,%s,%s,%s,%s,

                %s,%s,%s,%s,%s,

                %s,%s,%s,%s,%s,%s

            )

            """,

            (

                checkpoint_id,

                shipment_id,

                tracking["tracking_id"],

                tracking["vehicle_id"],

                tracking["driver_id"],

                checkpoint_sequence,

                latitude,

                longitude,

                location,

                distance,

                delay,

                traffic,

                weather,

                "REACHED",

                now,

                tracking["correlation_id"]

            )

        )



        # ------------------------------------------------
        # Build Event Payload
        # ------------------------------------------------


        payload = {

            "checkpoint_id":
                checkpoint_id,


            "shipment_id":
                shipment_id,


            "tracking_id":
                tracking["tracking_id"],


            "vehicle_id":
                tracking["vehicle_id"],


            "driver_id":
                tracking["driver_id"],


            "checkpoint_sequence":
                checkpoint_sequence,


            "location":
                location,


            "latitude":
                float(latitude),


            "longitude":
                float(longitude),


            "distance_completed_km":
                float(distance),


            "traffic_status":
                traffic,


            "weather_condition":
                weather,


            "delay_minutes":
                delay

        }



        # ------------------------------------------------
        # Publish Outbox
        # ------------------------------------------------


        publish_event(

            db=db,

            event_type="CheckpointReached",

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=str(
                tracking["correlation_id"]
            ),

            payload=payload

        )



        # ------------------------------------------------
        # Log
        # ------------------------------------------------


        log_event_success(

            EVENT_NAME,

            {

                "shipment_id":
                    shipment_id,


                "tracking_id":
                    tracking["tracking_id"],


                "checkpoint":
                    checkpoint_sequence,


                "location":
                    location,


                "delay":
                    delay

            }

        )




if __name__ == "__main__":


    try:

        generate_checkpoint()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise