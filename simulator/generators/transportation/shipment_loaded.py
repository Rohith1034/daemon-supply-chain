from datetime import datetime, timezone
import uuid


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentLoaded"



def load_shipment():


    with Database() as db:


        # ---------------------------------
        # Find shipment ready for loading
        # ---------------------------------

        shipment = db.fetch_one(

            """

            SELECT

                s.*,

                st.vehicle_id,

                st.trailer_id


            FROM shipments s


            JOIN shipment_transportation st

            ON s.shipment_id = st.shipment_id


            WHERE st.status='DRIVER_ASSIGNED'


            ORDER BY s.created_at


            LIMIT 1

            """

        )


        if not shipment:

            raise Exception(
                "No shipment waiting for loading"
            )



        shipment_id = shipment["shipment_id"]



        # ---------------------------------
        # Calculate quantity
        # ---------------------------------

        quantity = db.fetch_one(

            """

            SELECT

            SUM(shipped_quantity) AS qty


            FROM shipment_items


            WHERE shipment_id=%s

            """,

            (
                shipment_id,
            )

        )


        loaded_quantity = quantity["qty"]



        loaded_time = datetime.now(
            timezone.utc
        )



        loading_id = (

            "LOAD" +

            str(uuid.uuid4())
            .replace("-","")[:24]

        )



        # ---------------------------------
        # Insert loading event
        # ---------------------------------

        db.execute(

            """

            INSERT INTO shipment_loading_events

            (

            loading_id,

            shipment_id,

            warehouse_id,

            vehicle_id,

            trailer_id,

            loaded_quantity,

            loading_status,

            loaded_at

            )


            VALUES

            (%s,%s,%s,%s,%s,%s,%s,%s)

            """,

            (

            loading_id,

            shipment_id,

            shipment["warehouse_id"],

            shipment["vehicle_id"],

            shipment["trailer_id"],

            loaded_quantity,

            "LOADED",

            loaded_time

            )

        )



        # ---------------------------------
        # Update transportation status
        # ---------------------------------

        db.execute(

            """

            UPDATE shipment_transportation

            SET

            status='LOADED'


            WHERE shipment_id=%s

            """,

            (

            shipment_id,

            )

        )



        # ---------------------------------
        # Update shipment
        # ---------------------------------

        db.execute(

            """

            UPDATE shipments

            SET

            shipment_status='LOADED',

            updated_at=%s


            WHERE shipment_id=%s


            """,

            (

            loaded_time,

            shipment_id

            )

        )



        # ---------------------------------
        # Payload
        # ---------------------------------

        payload = {


            "shipment_id":

                shipment_id,


            "vehicle_id":

                shipment["vehicle_id"],


            "trailer_id":

                shipment["trailer_id"],


            "warehouse_id":

                shipment["warehouse_id"],


            "loaded_quantity":

                loaded_quantity,


            "loaded_at":

                loaded_time.isoformat()


        }



        # ---------------------------------
        # Outbox
        # ---------------------------------

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


            "vehicle_id":

                shipment["vehicle_id"],


            "trailer_id":

                shipment["trailer_id"],


            "quantity":

                loaded_quantity

            }

        )





if __name__ == "__main__":


    try:

        load_shipment()


    except Exception as e:

        log_event_failure(

            EVENT_NAME,

            e

        )

        raise