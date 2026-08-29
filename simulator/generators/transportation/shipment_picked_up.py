from datetime import datetime, timezone
import uuid

from core.db import Database
from core.ids import next_loading_id, next_tracking_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentPickedUp"



def generate_shipment_picked_up():

    with Database() as db:

        now = datetime.now(timezone.utc)


        # -----------------------------------
        # Find assigned shipment
        # -----------------------------------

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

            WHERE s.shipment_status='ASSIGNED'
            LIMIT 1
            """
        )


        if not shipment:
            raise Exception(
                "No ASSIGNED shipment found"
            )


        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        vehicle_id = shipment["vehicle_id"]

        trailer_id = shipment["trailer_id"]

        driver_id = shipment["driver_id"]

        correlation_id = str(
            shipment["correlation_id"]
            or uuid.uuid4()
        )


        # -----------------------------------
        # Loading event
        # -----------------------------------

        loading_id = next_loading_id(db)


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
                loaded_at,
                correlation_id
            )

            VALUES
            (
                %s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,

            (

                loading_id,

                shipment_id,

                warehouse_id,

                vehicle_id,

                trailer_id,

                shipment["total_quantity"],

                "COMPLETED",

                now,

                correlation_id

            )
        )



        # -----------------------------------
        # Shipment update
        # -----------------------------------

        db.execute(
            """
            UPDATE shipments

            SET

            shipment_status='PICKED_UP',

            updated_at=%s,

            correlation_id=%s

            WHERE shipment_id=%s

            """,

            (
                now,
                correlation_id,
                shipment_id
            )
        )



        # -----------------------------------
        # Vehicle status
        # -----------------------------------

        db.execute(
            """
            UPDATE vehicles

            SET status='IN_TRANSIT'

            WHERE vehicle_id=%s
            """,

            (
                vehicle_id,
            )
        )



        # -----------------------------------
        # Tracking
        # -----------------------------------

        tracking_id = next_tracking_id(db)


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
                correlation_id
            )

            VALUES
            (
                %s,%s,%s,%s,%s,%s,%s
            )

            """,

            (

                tracking_id,

                shipment_id,

                vehicle_id,

                driver_id,

                "PICKED_UP",

                now,

                correlation_id

            )
        )



        # -----------------------------------
        # Event Payload
        # -----------------------------------

        payload = {


            "eventType": EVENT_NAME,

            "occurredAt":
                now.isoformat(),


            "shipment": {


                "shipmentId":
                    shipment_id,


                "warehouseId":
                    warehouse_id,


                "vehicleId":
                    vehicle_id,


                "trailerId":
                    trailer_id,


                "driverId":
                    driver_id,


                "status":
                    "PICKED_UP"


            },


            "correlationId":
                str(correlation_id)

        }



        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=str(correlation_id),

            payload=payload

        )



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

                "status":
                    "PICKED_UP",

                "correlation_id":
                    correlation_id

            }

        )




if __name__ == "__main__":

    try:

        generate_shipment_picked_up()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise