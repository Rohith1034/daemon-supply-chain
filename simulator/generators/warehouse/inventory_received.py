from datetime import datetime, timezone
import random

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_completed_receiving_task(cursor):

    cursor.execute(
        """
        SELECT id, payload

        FROM event_outbox

        WHERE event_type='TaskCompleted'

        AND status='PENDING'

        ORDER BY created_at

        LIMIT 1

        FOR UPDATE SKIP LOCKED
        """
    )

    return cursor.fetchone()



def get_asn(cursor, shipment_id):

    cursor.execute(
        """
        SELECT id, payload

        FROM event_outbox

        WHERE event_type='ASNReceived'

        AND aggregate_id=%s

        AND status='PENDING'

        ORDER BY created_at DESC

        LIMIT 1

        FOR UPDATE SKIP LOCKED

        """,
        (
            shipment_id,
        )
    )

    return cursor.fetchone()



def create_inventory_received():


    conn=get_connection()
    cursor=conn.cursor()


    try:


        task=get_completed_receiving_task(cursor)


        if not task:

            print(
                "No completed receiving task"
            )

            return



        source_event_id=task[0]
        task_payload=task[1]


        task_id=task_payload["task"]["task_id"]



        shipment_id=(
            task_id.replace(
                "TASK-",
                ""
            )
        )



        asn=get_asn(
            cursor,
            shipment_id
        )


        if not asn:

            print(
                "ASN missing"
            )

            return



        asn_event_id=asn[0]
        asn_payload=asn[1]["asn"]

        po_id=asn_payload["po_id"]

        cursor.execute(
            """
            SELECT 1
            FROM event_outbox
            WHERE event_type='InventoryReceived'
            AND aggregate_id=%s
            LIMIT 1
            """,
            (shipment_id,)
        )

        if cursor.fetchone():
            conn.rollback()
            print("Inventory receipt already exists:", shipment_id)
            return



        now=datetime.now(
            timezone.utc
        )



        received_items=[]


        for item in asn_payload["items"]:


            damaged=random.randint(
                0,
                2
            )


            received_quantity=(
                item["quantity"]
                -
                damaged
            )


            received_items.append(

                {

                "product_id":
                    item["product_id"],


                "expected_quantity":
                    item["quantity"],


                "received_quantity":
                    received_quantity,


                "damaged_quantity":
                    damaged

                }

            )



        grn_id = (

            f"GRN-{shipment_id}"

        )



        correlation_id = (
            generate_correlation_id(
                po_id,
                prefix="PO"
            )
        )



        event={


            "event_id":
                generate_event_id(),


            "event_type":
                "InventoryReceived",


            "event_version":
                "1.0",


            "timestamp":
                now.isoformat(),


            "source":
                "warehouse-management-system",


            "aggregate_type":
                "SHIPMENT",


            "aggregate_id":
                shipment_id,


            "correlation_id":
                correlation_id,



            "goods_receipt":{


                "grn_id":
                    grn_id,


                "shipment_id":
                    shipment_id,

                "po_id":
                    po_id,


                "supplier_id":
                    asn_payload["supplier_id"],


                "warehouse_id":
                    asn_payload["warehouse_id"],


                "items":
                    received_items,


                "status":
                    "RECEIVED"

            }

        }




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



        cursor.execute(
            """
            UPDATE event_outbox
            SET status='PROCESSED'
            WHERE id=%s
            """,
            (source_event_id,)
        )

        conn.commit()


        print(
            "Inventory Received:",
            grn_id
        )


        return event



    except Exception as e:

        conn.rollback()

        raise e


    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":

    create_inventory_received()