from datetime import datetime, timezone

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "StockIncreased"



def generate_stock_increased():

    with Database() as db:


        # ------------------------------------
        # Find latest received shipment
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id
            FROM shipments
            WHERE shipment_status='RECEIVED'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )


        if not shipment:

            raise Exception(
                "No RECEIVED shipment found"
            )


        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )


        now = datetime.now(
            timezone.utc
        )



        # ------------------------------------
        # Calculate received quantity
        # ------------------------------------

        result = db.fetch_one(
            """
            SELECT
                SUM(quantity) AS total_received
            FROM inventory_transactions

            WHERE shipment_id=%s

            AND transaction_type='STOCK_RECEIVED'
            """,
            (
                shipment_id,
            )
        )


        total_received = result["total_received"]


        if not total_received:

            raise Exception(
                "No STOCK_RECEIVED transactions found"
            )



        # ------------------------------------
        # Payload
        # ------------------------------------

        payload = {


            "eventType":
                EVENT_NAME,


            "occurredAt":
                now.isoformat(),


            "inventory":

            {

                "shipmentId":
                    shipment_id,


                "warehouseId":
                    warehouse_id,


                "increasedQuantity":
                    total_received,


                "status":
                    "AVAILABLE"

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

            aggregate_type="INVENTORY",

            aggregate_id=shipment_id,

            correlation_id=correlation_id,

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {


                "shipment_id":
                    shipment_id,


                "warehouse_id":
                    warehouse_id,


                "quantity":
                    total_received,


                "correlation_id":
                    correlation_id


            }

        )




if __name__ == "__main__":

    try:

        generate_stock_increased()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise