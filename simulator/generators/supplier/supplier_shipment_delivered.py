from datetime import datetime, timezone


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)



EVENT_NAME = "SupplierShipmentDelivered"



def deliver_supplier_shipment():


    with Database() as db:


        # ---------------------------------------
        # Find ASN received shipment
        # ---------------------------------------

        shipment = db.fetch_one(

            """

            SELECT

                shipment_id,

                po_id,

                supplier_id,

                warehouse_id,

                correlation_id


            FROM shipments


            WHERE shipment_status='ASN_RECEIVED'


            ORDER BY shipment_date


            LIMIT 1


            """

        )



        if not shipment:


            raise Exception(

                "No ASN_RECEIVED shipment found"

            )



        shipment_id = shipment["shipment_id"]

        po_id = shipment["po_id"]

        supplier_id = shipment["supplier_id"]

        warehouse_id = shipment["warehouse_id"]

        
        correlation_id = str(
            shipment["correlation_id"]
            or "00000000-0000-0000-0000-000000000000"
        )



        now = datetime.now(

            timezone.utc

        )




        # ---------------------------------------
        # Update shipment status
        # ---------------------------------------

        db.execute(

            """

            UPDATE shipments

            SET

                shipment_status=%s,

                updated_at=%s


            WHERE shipment_id=%s


            """,

            (

                "DELIVERED",

                now,

                shipment_id

            )

        )




        # ---------------------------------------
        # Event Payload
        # ---------------------------------------

        payload = {


            "event_type":

                EVENT_NAME,


            "shipment_id":

                shipment_id,


            "po_id":

                po_id,


            "supplier_id":

                supplier_id,


            "warehouse_id":

                warehouse_id,


            "status":

                "DELIVERED",


            "delivered_at":

                now.isoformat(),


            "correlation_id":

                correlation_id


        }




        # ---------------------------------------
        # Outbox
        # ---------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=correlation_id,

            payload=payload

        )




        # ---------------------------------------
        # Logging
        # ---------------------------------------

        log_event_success(

            EVENT_NAME,

            {


                "shipment_id":

                    shipment_id,


                "po_id":

                    po_id,


                "supplier_id":

                    supplier_id,


                "warehouse_id":

                    warehouse_id,


                "correlation_id":

                    correlation_id


            }

        )



        print(

f"""

============================================================
EVENT : {EVENT_NAME}

SHIPMENT ID         : {shipment_id}
PO ID               : {po_id}
SUPPLIER ID         : {supplier_id}
WAREHOUSE ID        : {warehouse_id}

CORRELATION ID      : {correlation_id}

TIME : {now}

STATUS : SUCCESS
============================================================

"""

        )





if __name__ == "__main__":


    try:

        deliver_supplier_shipment()


    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )


        raise