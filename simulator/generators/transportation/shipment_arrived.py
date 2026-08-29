from datetime import datetime, timezone

from core.db import Database

from core.outbox import publish_event


from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentArrived"



def generate_shipment_arrived():

    with Database() as db:


        # ---------------------------------
        # Get dispatched shipment
        # ---------------------------------

        shipment = db.fetch_one(

            """
            SELECT
                s.shipment_id,
                s.po_id,
                s.supplier_id,
                s.warehouse_id,
                s.total_quantity,
                s.correlation_id,
                st.tracking_id,
                st.vehicle_id,
                st.driver_id

            FROM shipments s

            JOIN shipment_tracking st

            ON s.shipment_id = st.shipment_id


            WHERE

                s.shipment_status = 'IN_TRANSIT'

            LIMIT 1

            """

        )


        if not shipment:

            raise Exception(
                "No IN_TRANSIT shipment found"
            )


        now = datetime.now(
            timezone.utc
        )


        # ---------------------------------
        # Update shipment
        # ---------------------------------

        db.execute(

            """

            UPDATE shipments

            SET

                shipment_status = 'ARRIVED',

                actual_delivery = %s,

                updated_at = %s


            WHERE shipment_id = %s

            """,

            (

                now,

                now,

                shipment["shipment_id"]

            )

        )



        # ---------------------------------
        # Update tracking
        # ---------------------------------

        db.execute(

            """

            UPDATE shipment_tracking

            SET

                status = 'ARRIVED',

                actual_arrival = %s


            WHERE tracking_id = %s

            """,

            (

                now,

                shipment["tracking_id"]

            )

        )



        # ---------------------------------
        # Event Payload
        # ---------------------------------

        payload = {


            "event_type":
                EVENT_NAME,


            "shipment_id":
                shipment["shipment_id"],


            "po_id":
                shipment["po_id"],


            "supplier_id":
                shipment["supplier_id"],


            "warehouse_id":
                shipment["warehouse_id"],


            "tracking_id":
                shipment["tracking_id"],


            "vehicle_id":
                shipment["vehicle_id"],


            "driver_id":
                shipment["driver_id"],


            "quantity":
                shipment["total_quantity"],


            "arrival_time":
                now.isoformat(),


            "correlation_id":
                str(
                    shipment["correlation_id"]
                )

        }



        # ---------------------------------
        # Outbox
        # ---------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=
                shipment["shipment_id"],

            correlation_id=
                str(
                    shipment["correlation_id"]
                ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

                "shipment_id":
                    shipment["shipment_id"],


                "tracking_id":
                    shipment["tracking_id"],


                "warehouse_id":
                    shipment["warehouse_id"],


                "quantity":
                    shipment["total_quantity"]

            }

        )





if __name__ == "__main__":


    try:

        generate_shipment_arrived()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise