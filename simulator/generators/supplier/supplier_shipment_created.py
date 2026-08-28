from datetime import datetime, timezone, timedelta
import uuid

from psycopg2.extras import Json, execute_values

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



# --------------------------------------------------
# ID Generator
# --------------------------------------------------

def generate_shipment_id(cursor):
    return f"SHIP-{uuid.uuid4().hex[:8].upper()}"



# --------------------------------------------------
# Get PO
# --------------------------------------------------

def get_ready_po(cursor):

    cursor.execute(
        """
        SELECT

            po_id,
            supplier_id,
            warehouse_id

        FROM purchase_orders

        WHERE po_status='ACKNOWLEDGED'

        ORDER BY order_date

        LIMIT 1

        """
    )

    return cursor.fetchone()



# --------------------------------------------------
# Get PO Items
# --------------------------------------------------

def get_po_items(
        cursor,
        po_id
):

    cursor.execute(
        """
        SELECT

            product_id,
            ordered_quantity,
            unit_cost


        FROM purchase_order_items


        WHERE po_id=%s


        """,
        (
            po_id,
        )
    )

    return cursor.fetchall()



# --------------------------------------------------
# Get Supplier
# --------------------------------------------------

def get_supplier(
        cursor,
        supplier_id
):

    cursor.execute(
        """
        SELECT

            supplier_id,
            country,
            city,
            lead_time_days


        FROM suppliers


        WHERE supplier_id=%s


        """,
        (
            supplier_id,
        )
    )

    return cursor.fetchone()



# --------------------------------------------------
# Get Warehouse
# --------------------------------------------------

def get_warehouse(
        cursor,
        warehouse_id
):

    cursor.execute(
        """
        SELECT

            warehouse_id,
            country,
            city


        FROM warehouses


        WHERE warehouse_id=%s


        """,
        (
            warehouse_id,
        )
    )


    return cursor.fetchone()



# --------------------------------------------------
# Generate Shipment
# --------------------------------------------------

def create_supplier_shipment():


    conn = get_connection()

    cursor = conn.cursor()


    try:


        po = get_ready_po(cursor)


        if not po:

            print(
                "No PO ready for shipment"
            )

            return



        po_id = po[0]

        supplier_id = po[1]

        warehouse_id = po[2]



        supplier = get_supplier(
            cursor,
            supplier_id
        )


        warehouse = get_warehouse(
            cursor,
            warehouse_id
        )



        items = get_po_items(
            cursor,
            po_id
        )



        shipment_id = generate_shipment_id(
            cursor
        )



        now = datetime.now(
            timezone.utc
        )



        expected_delivery = (

            now +

            timedelta(
                days=supplier[3]
            )

        )



        total_quantity = sum(

            item[1]

            for item in items

        )



        correlation_id = (

            generate_correlation_id(
                po_id,
                prefix="PO"
            )

        )



        # --------------------------------
        # Insert Shipment
        # --------------------------------


        cursor.execute(

        """

        INSERT INTO shipments

        (

        shipment_id,
        po_id,
        supplier_id,
        warehouse_id,
        shipment_status,
        shipment_date,
        expected_delivery,
        total_skus,
        total_quantity

        )


        VALUES

        (%s,%s,%s,%s,%s,%s,%s,%s,%s)

        """,

        (

        shipment_id,

        po_id,

        supplier_id,

        warehouse_id,

        "CREATED",

        now,

        expected_delivery,

        len(items),

        total_quantity

        )

        )



        # --------------------------------
        # Shipment Items
        # --------------------------------


        shipment_items=[]


        for item in items:


            shipment_items.append(

                (

                shipment_id,

                item[0],

                item[1],

                0,

                0

                )

            )



        execute_values(

            cursor,

            """

            INSERT INTO shipment_items

            (

            shipment_id,

            product_id,

            shipped_quantity,

            received_quantity,

            damaged_quantity

            )


            VALUES %s


            """,

            shipment_items

        )




        # --------------------------------
        # Event
        # --------------------------------


        event = {


            "event_id":
                generate_event_id(),


            "event_type":
                "SupplierShipmentCreated",


            "event_version":
                "1.0",


            "timestamp":
                now.isoformat(),


            "source":
                "supplier-shipment-service",



            "aggregate_type":
                "SHIPMENT",


            "aggregate_id":
                shipment_id,



            "correlation_id":
                correlation_id,



            "shipment":{


                "shipment_id":
                    shipment_id,


                "po_id":
                    po_id,


                "supplier_id":
                    supplier_id,


                "warehouse_id":
                    warehouse_id,



                "origin":{

                    "country":
                        supplier[1],

                    "city":
                        supplier[2]

                },


                "destination":{

                    "country":
                        warehouse[1],

                    "city":
                        warehouse[2]

                },


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


                "shipment_status":
                    "CREATED"


            }


        }



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



        # Update PO

        cursor.execute(

        """

        UPDATE purchase_orders

        SET po_status='SHIPPED',

            updated_at=%s


        WHERE po_id=%s


        """,

        (

        now,

        po_id

        )

        )



        conn.commit()


        print(
            "Shipment Created:",
            shipment_id
        )


        return event



    except Exception as e:


        conn.rollback()

        raise e



    finally:


        cursor.close()

        conn.close()



if __name__=="__main__":

    create_supplier_shipment()