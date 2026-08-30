from datetime import datetime, timezone


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "ShipmentReady"



def generate_shipment_ready():


    with Database() as db:


        package = db.fetch_one(

            """
            SELECT
                package_id,
                order_id,
                warehouse_id,
                correlation_id

            FROM packages

            WHERE package_status='PACKED'

            ORDER BY packed_at DESC

            LIMIT 1
            """

        )


        if not package:

            raise Exception(
                "No packed package found"
            )



        package_id = package["package_id"]

        warehouse_id = package["warehouse_id"]

        correlation_id = package["correlation_id"]


        now = datetime.now(
            timezone.utc
        )



        # --------------------------------
        # Find shipment
        # --------------------------------

        shipment = db.fetch_one(

            """
            SELECT
                shipment_id

            FROM shipments

            WHERE warehouse_id=%s

            ORDER BY created_at

            LIMIT 1

            """,

            (
                warehouse_id,
            )

        )


        if not shipment:

            raise Exception(
                "No shipment found"
            )



        shipment_id = shipment["shipment_id"]



        # --------------------------------
        # Update shipment
        # --------------------------------

        db.execute(

            """
            UPDATE shipments

            SET

                shipment_status='READY',

                updated_at=%s

            WHERE shipment_id=%s

            """,

            (

                now,

                shipment_id

            )

        )



        # --------------------------------
        # Event payload
        # --------------------------------

        payload = {


            "eventType":
                EVENT_NAME,


            "occurredAt":
                now.isoformat(),


            "shipment":

            {

                "shipmentId":
                    shipment_id,


                "packageId":
                    package_id,


                "warehouseId":
                    warehouse_id,


                "status":
                    "READY"

            },


            "correlationId":
                str(correlation_id)

        }



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

                "package_id":
                    package_id,

                "warehouse_id":
                    warehouse_id,

                "status":
                    "READY",

                "correlation_id":
                    correlation_id

            }

        )





if __name__ == "__main__":


    try:

        generate_shipment_ready()


    except Exception as e:

        log_event_failure(

            EVENT_NAME,

            e

        )

        raise