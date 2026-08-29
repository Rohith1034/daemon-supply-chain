from datetime import datetime, timezone
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)



EVENT_NAME = "DriverAssigned"



def assign_driver():


    with Database() as db:


        # ----------------------------------
        # Find shipment with vehicle assigned
        # ----------------------------------

        transportation = db.fetch_one(

            """
            SELECT *
            FROM shipment_transportation

            WHERE status='VEHICLE_ASSIGNED'

            ORDER BY assigned_at

            LIMIT 1
            """

        )


        if not transportation:

            raise Exception(
                "No shipment waiting for driver assignment"
            )



        shipment_id = transportation["shipment_id"]



        # ----------------------------------
        # Select active driver
        # ----------------------------------

        driver = db.fetch_one(

            """

            SELECT *

            FROM drivers

            WHERE status='ACTIVE'

            ORDER BY random()

            LIMIT 1

            """

        )


        if not driver:

            raise Exception(
                "No active driver available"
            )



        assigned_time = datetime.now(
            timezone.utc
        )



        # ----------------------------------
        # Update transportation assignment
        # ----------------------------------

        db.execute(

            """

            UPDATE shipment_transportation

            SET

                driver_id=%s,

                status='DRIVER_ASSIGNED',

                assigned_at=%s


            WHERE shipment_id=%s

            """

            ,

            (

                driver["driver_id"],

                assigned_time,

                shipment_id

            )

        )



        # ----------------------------------
        # Update shipment status
        # ----------------------------------

        db.execute(

            """

            UPDATE shipments

            SET

                shipment_status='DRIVER_ASSIGNED',

                updated_at=%s


            WHERE shipment_id=%s

            """

            ,

            (

                assigned_time,

                shipment_id

            )

        )



        # ----------------------------------
        # Fetch complete assignment
        # ----------------------------------

        assignment = db.fetch_one(

            """

            SELECT

                st.*,

                v.vehicle_number,

                t.trailer_number,

                d.driver_name


            FROM shipment_transportation st


            JOIN vehicles v

            ON st.vehicle_id=v.vehicle_id


            JOIN trailers t

            ON st.trailer_id=t.trailer_id


            JOIN drivers d

            ON st.driver_id=d.driver_id


            WHERE st.shipment_id=%s

            """

            ,

            (

                shipment_id,

            )

        )



        # ----------------------------------
        # Event payload
        # ----------------------------------

        payload = {


            "event_type":

                EVENT_NAME,


            "shipment_id":

                shipment_id,


            "vehicle_id":

                assignment["vehicle_id"],


            "vehicle_number":

                assignment["vehicle_number"],


            "trailer_id":

                assignment["trailer_id"],


            "trailer_number":

                assignment["trailer_number"],


            "driver_id":

                assignment["driver_id"],


            "driver_name":

                assignment["driver_name"],


            "assigned_at":

                assigned_time.isoformat()

        }



        # ----------------------------------
        # Publish outbox
        # ----------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=None,

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {


                "shipment_id":

                    shipment_id,


                "driver_id":

                    driver["driver_id"],


                "vehicle_id":

                    assignment["vehicle_id"],


                "trailer_id":

                    assignment["trailer_id"]

            }

        )





if __name__ == "__main__":


    try:

        assign_driver()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise