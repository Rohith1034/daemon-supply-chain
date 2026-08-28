from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def check_reorder():


    conn=get_connection()
    cursor=conn.cursor()


    try:


        cursor.execute("""

        SELECT

        product_id,
        warehouse_id,
        available_quantity,
        reorder_point,
        reorder_quantity


        FROM inventory


        WHERE available_quantity
        <= reorder_point


        LIMIT 1


        """)


        row=cursor.fetchone()


        if not row:

            print(
            "No reorder required"
            )

            return



        product_id=row[0]
        warehouse_id=row[1]


        now=datetime.now(
            timezone.utc
        )


        correlation_id=generate_correlation_id(
            product_id,
            prefix="SKU"
        )


        event={


        "event_id":
            generate_event_id(),


        "event_type":
            "ReorderTriggered",


        "timestamp":
            now.isoformat(),


        "correlation_id":
            correlation_id,


        "reorder":

        {

        "product_id":
            product_id,


        "warehouse_id":
            warehouse_id,


        "current_stock":
            row[2],


        "reorder_point":
            row[3],


        "recommended_quantity":
            row[4]

        }

        }


        cursor.execute(
        """

        INSERT INTO event_outbox

        VALUES
        (
        DEFAULT,
        %s,
        %s,
        %s,
        %s,
        %s,
        NOW(),
        NULL,
        'PENDING'
        )

        """,

        (

        event["event_id"],

        "ReorderTriggered",

        "INVENTORY",

        product_id,

        correlation_id,

        Json(event)

        ))



        conn.commit()


        print(
        "Reorder:",
        product_id
        )


    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":
    check_reorder()