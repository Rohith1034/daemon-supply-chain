from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)


def get_po_waiting_supplier_ack(cursor):


    cursor.execute(

        """

        SELECT


            po_id,

            supplier_id,

            warehouse_id


        FROM purchase_orders


        WHERE po_status='APPROVED'


        ORDER BY order_date


        LIMIT 1


        """

    )


    return cursor.fetchone()




def create_purchase_order_acknowledged():


    conn = get_connection()

    cursor = conn.cursor()



    try:



        po = get_po_waiting_supplier_ack(
            cursor
        )



        if not po:


            print(
                "No PO waiting for supplier acknowledgement"
            )

            return



        po_id = po[0]

        supplier_id = po[1]

        warehouse_id = po[2]



        now = datetime.now(
            timezone.utc
        )



        correlation_id = (

            generate_correlation_id(
                po_id,
                prefix="PO"
            )

        )



        supplier_reference = (

            f"SUP-ACK-{po_id}"

        )



        event = {



            "event_id":

                generate_event_id(),



            "event_type":

                "PurchaseOrderAcknowledged",



            "event_version":

                "1.0",



            "timestamp":

                now.isoformat(),



            "source":

                "supplier-system-simulator",



            "aggregate_type":

                "PURCHASE_ORDER",



            "aggregate_id":

                po_id,



            "correlation_id":

                correlation_id,



            "purchase_order":{


                "po_id":

                    po_id,


                "supplier_id":

                    supplier_id,


                "warehouse_id":

                    warehouse_id,


                "supplier_response":{


                    "status":

                        "ACCEPTED",


                    "supplier_reference":

                        supplier_reference,


                    "acknowledged_at":

                        now.isoformat()

                }

            }


        }



        # Update PO


        cursor.execute(

        """

        UPDATE purchase_orders


        SET

        po_status='ACKNOWLEDGED',

        updated_at=%s


        WHERE po_id=%s


        """,

        (

        now,

        po_id

        )

        )





        # Save event


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

        "PURCHASE_ORDER",

        po_id,

        correlation_id,

        Json(event)

        )

        )



        conn.commit()



        print(

            "PO Acknowledged:",

            po_id

        )



        return event



    except Exception as e:


        conn.rollback()

        raise e



    finally:


        cursor.close()

        conn.close()




if __name__=="__main__":

    create_purchase_order_acknowledged()