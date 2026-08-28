from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)

def get_pending_asn(cursor):

    cursor.execute(
        """
        SELECT
            id,
            aggregate_id,
            payload

        FROM event_outbox

        WHERE event_type='ASNReceived'

        AND status='PENDING'

        ORDER BY created_at

        LIMIT 1

        FOR UPDATE SKIP LOCKED
        """
    )

    return cursor.fetchone()



def create_task():

    conn=get_connection()
    cursor=conn.cursor()


    try:

        asn=get_pending_asn(cursor)


        if not asn:
            print("No ASN available")
            return



        source_event_id=asn[0]
        shipment_id=asn[1]

        payload=asn[2]["asn"]


        task_id=f"TASK-{shipment_id}"

        cursor.execute(
            """
            SELECT 1
            FROM event_outbox
            WHERE event_type='WarehouseTaskCreated'
            AND aggregate_id=%s
            LIMIT 1
            """,
            (task_id,)
        )

        if cursor.fetchone():
            conn.rollback()
            print("Warehouse task already exists:", task_id)
            return

        now=datetime.now(timezone.utc)


        correlation_id=generate_correlation_id(
            shipment_id,
            prefix="ORDER"
        )



        event={

            "event_id":
                generate_event_id(),

            "event_type":
                "WarehouseTaskCreated",

            "event_version":"1.0",

            "timestamp":
                now.isoformat(),

            "source":
                "warehouse-task-service",

            "aggregate_type":
                "WAREHOUSE_TASK",

            "aggregate_id":
                task_id,

            "correlation_id":
                correlation_id,


            "task":{

                "task_id":
                    task_id,

                "task_type":
                    "RECEIVING",

                "warehouse_id":
                    payload["warehouse_id"],

                "shipment_id":
                    shipment_id,

                "priority":
                    "HIGH",

                "required_skill":
                    "RECEIVING",

                "total_quantity":
                    payload["total_quantity"],

                "status":
                    "CREATED"
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
            "WAREHOUSE_TASK",
            task_id,
            correlation_id,
            Json(event)
        ))

        conn.commit()

        print(
            "Created task:",
            task_id
        )


    except Exception as e:

        conn.rollback()
        raise e


    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":
    create_task()