from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



# ------------------------------------------
# Get created shipment
# ------------------------------------------

def get_created_shipment(cursor):

    cursor.execute(
        """
        SELECT

            shipment_id,
            po_id,
            supplier_id,
            warehouse_id

        FROM shipments


        WHERE shipment_status='CREATED'


        ORDER BY shipment_date


        LIMIT 1

        FOR UPDATE

        """
    )

    return cursor.fetchone()



# ------------------------------------------
# Get shipment items
# ------------------------------------------

def get_shipment_items(
    cursor,
    shipment_id
):

    cursor.execute(
        """
        SELECT
            product_id,
            shipped_quantity
        FROM shipment_items
        WHERE shipment_id=%s
        """,
        (
            shipment_id,
        )
    )

    return cursor.fetchall()



# ------------------------------------------
# ASN Generator
# ------------------------------------------

def create_asn_received():


    conn = get_connection()

    cursor = conn.cursor()


    try:


        shipment = get_created_shipment(
            cursor
        )


        if not shipment:

            print(
                "No shipment waiting for ASN"
            )

            return



        shipment_id = shipment[0]

        po_id = shipment[1]

        supplier_id = shipment[2]

        warehouse_id = shipment[3]



        items = get_shipment_items(
            cursor,
            shipment_id
        )



        now = datetime.now(
            timezone.utc
        )



        correlation_id = (

            generate_correlation_id(
                po_id,
                prefix="PO"
            )

        )



        asn_id = (

            f"ASN-{shipment_id}"

        )



        total_quantity = sum(

            item[1]

            for item in items

        )



        event = {


            "event_id":

                generate_event_id(),



            "event_type":

                "ASNReceived",



            "event_version":

                "1.0",



            "timestamp":

                now.isoformat(),



            "source":

                "supplier-integration-service",



            "aggregate_type":

                "SHIPMENT",



            "aggregate_id":

                shipment_id,



            "correlation_id":

                correlation_id,



            "asn":{


                "asn_id":

                    asn_id,


                "shipment_id":

                    shipment_id,


                "po_id":

                    po_id,


                "supplier_id":

                    supplier_id,


                "warehouse_id":

                    warehouse_id,



                "items":[


                    {

                    "product_id":

                        item[0],


                    "quantity":

                        item[1]

                    }

                    for item in items


                ],



                "total_skus":

                    len(items),



                "total_quantity":

                    total_quantity,



                "status":

                    "RECEIVED"

            }

        }



        # --------------------------------
        # Update shipment
        # --------------------------------


        cursor.execute(

        """

        UPDATE shipments

        SET

        shipment_status='ASN_RECEIVED',

        updated_at=%s


        WHERE shipment_id=%s


        """,

        (

        now,

        shipment_id

        )

        )




        # --------------------------------
        # Event Outbox
        # --------------------------------


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

        )

        )


        conn.commit()


        print(
            "ASN Received:",
            asn_id
        )


        return event



    except Exception as e:

        conn.rollback()

        raise e



    finally:

        cursor.close()

        conn.close()



if __name__=="__main__":

    create_asn_received()