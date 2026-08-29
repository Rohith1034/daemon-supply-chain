from datetime import datetime, timezone


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "CarrierAssigned"



def generate_carrier_assigned():

    with Database() as db:


        # ---------------------------------------
        # Find READY shipment
        # ---------------------------------------

        shipment = db.fetch_one(

            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id

            FROM shipments

            WHERE shipment_status='READY'

            ORDER BY created_at

            LIMIT 1
            """

        )


        if not shipment:

            raise Exception(
                "No READY shipment found"
            )


        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = shipment["correlation_id"]



        # ---------------------------------------
        # Find active vehicle
        # ---------------------------------------

        vehicle = db.fetch_one(

            """
            SELECT
                vehicle_id

            FROM vehicles

            WHERE status='ACTIVE'

            LIMIT 1

            """

        )


        if not vehicle:

            raise Exception(
                "No ACTIVE vehicle found"
            )


        vehicle_id = vehicle["vehicle_id"]



        # ---------------------------------------
        # Find active trailer
        # ---------------------------------------

        trailer = db.fetch_one(

            """
            SELECT
                trailer_id

            FROM trailers

            WHERE status='ACTIVE'

            LIMIT 1

            """

        )


        if not trailer:

            raise Exception(
                "No ACTIVE trailer found"
            )


        trailer_id = trailer["trailer_id"]



        # ---------------------------------------
        # Find active driver
        # ---------------------------------------

        driver = db.fetch_one(

            """
            SELECT
                driver_id

            FROM drivers

            WHERE status='ACTIVE'

            LIMIT 1

            """

        )


        if not driver:

            raise Exception(
                "No ACTIVE driver found"
            )


        driver_id = driver["driver_id"]



        now = datetime.now(
            timezone.utc
        )



        # ---------------------------------------
        # Create transportation assignment
        # ---------------------------------------

        db.execute(

            """
            INSERT INTO shipment_transportation
            (

                shipment_id,

                vehicle_id,

                trailer_id,

                driver_id,

                assigned_at,

                status

            )

            VALUES

            (%s,%s,%s,%s,%s,%s)

            """,

            (

                shipment_id,

                vehicle_id,

                trailer_id,

                driver_id,

                now,

                "ASSIGNED"

            )

        )



        # ---------------------------------------
        # Update shipment
        # ---------------------------------------

        db.execute(

            """
            UPDATE shipments

            SET

                shipment_status='ASSIGNED',

                updated_at=%s

            WHERE shipment_id=%s

            """,

            (

                now,

                shipment_id

            )

        )



        # ---------------------------------------
        # Update resources
        # ---------------------------------------

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


        db.execute(

            """
            UPDATE trailers

            SET status='ASSIGNED'

            WHERE trailer_id=%s

            """,

            (

                trailer_id,

            )

        )


        db.execute(

            """
            UPDATE drivers

            SET status='ASSIGNED'

            WHERE driver_id=%s

            """,

            (

                driver_id,

            )

        )



        # ---------------------------------------
        # Event payload
        # ---------------------------------------

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


                "trailerId":

                    trailer_id,


                "driverId":

                    driver_id,


                "status":

                    "ASSIGNED"

            },


            "correlationId":

                str(correlation_id)

        }



        # ---------------------------------------
        # Kafka Outbox
        # ---------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=str(
                correlation_id
            ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

                "shipment_id":

                    shipment_id,


                "vehicle_id":

                    vehicle_id,


                "trailer_id":

                    trailer_id,


                "driver_id":

                    driver_id,


                "warehouse_id":

                    warehouse_id,


                "status":

                    "ASSIGNED",


                "correlation_id":

                    correlation_id

            }

        )





if __name__ == "__main__":


    try:

        generate_carrier_assigned()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )

        raise