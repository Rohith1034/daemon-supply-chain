from datetime import datetime, timezone
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "VehicleAssigned"



def assign_vehicle():


    with Database() as db:


        # ------------------------------------
        # Find shipment waiting for transport
        # ------------------------------------

        shipment = db.fetch_one(

            """
            SELECT *
            FROM shipments

            WHERE shipment_status='CREATED'

            ORDER BY created_at

            LIMIT 1

            """

        )


        if not shipment:

            raise Exception(
                "No shipment available for vehicle assignment"
            )



        shipment_id = shipment["shipment_id"]



        # ------------------------------------
        # Select available vehicle
        # ------------------------------------

        vehicle = db.fetch_one(

            """

            SELECT *

            FROM vehicles

            WHERE status='ACTIVE'

            ORDER BY random()

            LIMIT 1

            """

        )


        if not vehicle:

            raise Exception(
                "No active vehicle found"
            )



        # ------------------------------------
        # Select available trailer
        # ------------------------------------

        trailer = db.fetch_one(

            """

            SELECT *

            FROM trailers

            WHERE status='ACTIVE'

            ORDER BY random()

            LIMIT 1

            """

        )


        if not trailer:

            raise Exception(
                "No active trailer found"
            )



        assigned_time = datetime.now(
            timezone.utc
        )



        # ------------------------------------
        # Insert transportation assignment
        # ------------------------------------

        db.execute(

            """

            INSERT INTO shipment_transportation

            (

            shipment_id,

            vehicle_id,

            trailer_id,

            status,

            assigned_at

            )


            VALUES

            (%s,%s,%s,%s,%s)

            """

            ,

            (

            shipment_id,

            vehicle["vehicle_id"],

            trailer["trailer_id"],

            "VEHICLE_ASSIGNED",

            assigned_time

            )

        )



        # ------------------------------------
        # Update shipment status
        # ------------------------------------

        db.execute(

            """

            UPDATE shipments

            SET

            shipment_status='VEHICLE_ASSIGNED',

            updated_at=%s


            WHERE shipment_id=%s

            """,

            (

            assigned_time,

            shipment_id

            )

        )



        # ------------------------------------
        # Payload
        # ------------------------------------

        payload = {


            "event_type":

                EVENT_NAME,


            "shipment_id":

                shipment_id,


            "vehicle_id":

                vehicle["vehicle_id"],


            "vehicle_number":

                vehicle["vehicle_number"],


            "vehicle_type":

                vehicle["vehicle_type"],


            "trailer_id":

                trailer["trailer_id"],


            "trailer_number":

                trailer["trailer_number"],


            "assigned_at":

                assigned_time.isoformat(),


            "correlation_id":

                str(
                    shipment["correlation_id"]
                )
                if "correlation_id" in shipment
                else None

        }



        # ------------------------------------
        # Outbox
        # ------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=str(
                shipment.get("correlation_id")
            ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {


            "shipment_id":

                shipment_id,


            "vehicle_id":

                vehicle["vehicle_id"],


            "trailer_id":

                trailer["trailer_id"]

            }

        )





if __name__ == "__main__":


    try:

        assign_vehicle()


    except Exception as e:

        log_event_failure(

            EVENT_NAME,

            e

        )

        raise