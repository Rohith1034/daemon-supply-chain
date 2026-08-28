import random
from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)


def get_shipment(cursor):

    cursor.execute("""
        SELECT
            shipment_id,
                "aggregate_type":
                    "SHIPMENT",
                "aggregate_id":
                    shipment_id,
            po_id,
            supplier_id,
            warehouse_id,
            total_quantity
        FROM shipments
        WHERE shipment_status IN ('ASN_RECEIVED','IN_TRANSIT')
        ORDER BY random()
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """)

    return cursor.fetchone()



def create_supplier_shipment_delivered():

    conn = get_connection()
    cursor = conn.cursor()


    try:

        shipment = get_shipment(cursor)


        if not shipment:
            raise Exception(
                "No shipment available"
            )


        shipment_id = shipment[0]
        po_id = shipment[1]
        supplier_id = shipment[2]
        warehouse_id = shipment[3]
        quantity = shipment[4]


        now=datetime.now(
            timezone.utc
        )


        correlation_id = generate_correlation_id(
            po_id,
            prefix="PO"
        )


        event={

            "event_id":
                generate_event_id(),

            "event_type":
                "SupplierShipmentDelivered",

            "event_version":
                "1.0",

            "timestamp":
                now.isoformat(),

            "source":
                "supplier-service",


            "correlation_id":
                correlation_id,


            "shipment":

            {

                "shipment_id":
                    shipment_id,

                "po_id":
                    po_id,

                "supplier_id":
                    supplier_id,

                "warehouse_id":
                    warehouse_id,


                "received_quantity":
                    quantity,


                "delivery_status":
                    "DELIVERED",


                "delivery_time":
                    now.isoformat()

            }

        }


        cursor.execute(
        """

        UPDATE shipments

        SET
            shipment_status='DELIVERED',
            actual_delivery=%s,
            updated_at=%s

        WHERE shipment_id=%s

        """,
        (
            now,
            now,
            shipment_id
        )
        )


        cursor.execute(
        """

        INSERT INTO event_outbox

        (
            event_id,
            event_type,
            aggregate_type,
            aggregate_id,
            correlation_id,
            payload
        )

        VALUES
        (%s,%s,%s,%s,%s,%s)

        """,

        (

        event["event_id"],

        event["event_type"],

        "SHIPMENT",

        shipment_id,

        correlation_id,

        Json(event)

        ))

        conn.commit()


        print(
            "Delivered:",
            shipment_id
        )


    except Exception as e:

        conn.rollback()
        raise e


    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":

    create_supplier_shipment_delivered()