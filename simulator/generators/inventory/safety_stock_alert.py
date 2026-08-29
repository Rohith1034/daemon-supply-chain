from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def generate_alert():


    conn=get_connection()
    cursor=conn.cursor()


    cursor.execute("""

    SELECT

    i.product_id,
    i.warehouse_id,
    i.available_quantity,
    i.safety_stock


    FROM inventory i


    WHERE i.available_quantity < i.safety_stock
    AND NOT EXISTS (
        SELECT 1 FROM inventory_alerts
        WHERE product_id = i.product_id
        AND warehouse_id = i.warehouse_id
        AND alert_type = 'SAFETY_STOCK'
        AND alert_status = 'ACTIVE'
    )


    LIMIT 1

    """)


    row=cursor.fetchone()



    if not row:

        print(
        "No safety stock breach"
        )

        return



    product_id=row[0]
    warehouse_id=row[1]


    now=datetime.now(
        timezone.utc
    )


    event={


    "event_id":
        generate_event_id(),


    "event_type":
        "SafetyStockAlert",


    "timestamp":
        now.isoformat(),


    "correlation_id":
        generate_correlation_id(
            product_id
        ),


    "alert":

    {

    "product_id":
        product_id,


    "warehouse_id":
        warehouse_id,


    "available_quantity":
        row[2],


    "safety_stock":
        row[3],


    "severity":
        "HIGH"

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

    "SafetyStockAlert",

    "INVENTORY",

    product_id,

    event["correlation_id"],

    Json(event)

    ))

    # Insert into inventory_alerts to track the alert
    cursor.execute(
        """
        INSERT INTO inventory_alerts
        (alert_id, product_id, warehouse_id, alert_type, alert_status, created_at)
        VALUES (%s, %s, %s, 'SAFETY_STOCK', 'ACTIVE', NOW())
        """,
        (f"ALERT-{product_id}-{warehouse_id}", product_id, warehouse_id)
    )

    conn.commit()


    cursor.close()
    conn.close()



if __name__=="__main__":
    generate_alert()