from datetime import datetime, timezone, timedelta
import uuid
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentDispatched"



def dispatch_shipment():


    with Database() as db:


        # ------------------------------------
        # Find loaded shipment
        # ------------------------------------

        shipment = db.fetch_one(

            """

            SELECT

                s.*,

                st.vehicle_id,

                st.trailer_id,

                st.driver_id


            FROM shipments s


            JOIN shipment_transportation st

            ON s.shipment_id = st.shipment_id


            WHERE st.status='LOADED'


            ORDER BY s.created_at


            LIMIT 1

            """

        )


        if not shipment:

            raise Exception(
                "No loaded shipment available"
            )



        shipment_id = shipment["shipment_id"]



        dispatch_time = datetime.now(
            timezone.utc
        )



        # ------------------------------------
        # Generate ETA
        # ------------------------------------

        estimated_arrival = (

            dispatch_time +

            timedelta(
                days=random.randint(1,5)
            )

        )



        tracking_id = (

            "TRK" +

            str(uuid.uuid4())
            .replace("-","")[:24]

        )



        # ------------------------------------
        # Create tracking record
        # ------------------------------------

        db.execute(

            """

            INSERT INTO shipment_tracking

            (

            tracking_id,

            shipment_id,

            vehicle_id,

            driver_id,

            status,

            departure_time,

            estimated_arrival

            )


            VALUES

            (%s,%s,%s,%s,%s,%s,%s)

            """,

            (

            tracking_id,

            shipment_id,

            shipment["vehicle_id"],

            shipment["driver_id"],

            "IN_TRANSIT",

            dispatch_time,

            estimated_arrival

            )

        )



        # ------------------------------------
        # Update transportation
        # ------------------------------------

        db.execute(

            """

            UPDATE shipment_transportation

            SET

            status='DISPATCHED'


            WHERE shipment_id=%s


            """,

            (

            shipment_id,

            )

        )



        # ------------------------------------
        # Update shipment
        # ------------------------------------

        db.execute(

            """

            UPDATE shipments

            SET

            shipment_status='IN_TRANSIT',

            shipment_date=%s,

            updated_at=%s


            WHERE shipment_id=%s


            """,

            (

            dispatch_time,

            dispatch_time,

            shipment_id

            )

        )



        # ------------------------------------
        # Event Payload
        # ------------------------------------

        payload = {


            "shipment_id":

                shipment_id,


            "vehicle_id":

                shipment["vehicle_id"],


            "driver_id":

                shipment["driver_id"],


            "trailer_id":

                shipment["trailer_id"],


            "tracking_id":

                tracking_id,


            "status":

                "IN_TRANSIT",


            "departure_time":

                dispatch_time.isoformat(),


            "estimated_arrival":

                estimated_arrival.isoformat()

        }



        # ------------------------------------
        # Outbox
        # ------------------------------------

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


            "tracking_id":

                tracking_id,


            "vehicle_id":

                shipment["vehicle_id"],


            "driver_id":

                shipment["driver_id"]

            }

        )





if __name__ == "__main__":


    try:

        dispatch_shipment()


    except Exception as e:

        log_event_failure(

            EVENT_NAME,

            e

        )

        raise