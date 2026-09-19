import random
import string
from datetime import timedelta, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "ShipmentReady"



# ============================================================
# HELPERS
# ============================================================

def _ensure_utc(value):

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )



def _generate_id(prefix):

    suffix = ''.join(
        random.choices(
            string.ascii_uppercase +
            string.digits,
            k=6
        )
    )

    return f"{prefix}-{suffix}"



# ============================================================
# MAIN EVENT
# ============================================================

def generate_shipment_ready():


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CREATING OUTBOUND SHIPMENT

============================================================
"""
        )



        # ====================================================
        # FIND PACKED PACKAGE
        # ====================================================

        package = db.fetch_one(
            """
            SELECT
                p.package_id,
                p.order_id,
                p.warehouse_id,
                p.correlation_id
            FROM packages p
            LEFT JOIN outbound_shipments os
            ON p.package_id = os.package_id
            WHERE p.package_status='PACKED'
            AND os.package_id IS NULL
            LIMIT 1
            """
        )


        if not package:

            raise Exception(
                "No PACKED package found"
            )



        package_id = package["package_id"]

        order_id = package["order_id"]

        warehouse_id = package["warehouse_id"]

        correlation_id = str(
            package["correlation_id"]
        )



        print(
            f"""
============================================================
PACKAGE FOUND

PACKAGE:
{package_id}

ORDER:
{order_id}

WAREHOUSE:
{warehouse_id}

============================================================
"""
        )



        # ====================================================
        # FIND OR CREATE FULFILLMENT
        # ====================================================

        fulfillment = db.fetch_one(
            """
            SELECT
                fulfillment_id,
                status
            FROM outbound_fulfillment
            WHERE order_id=%s
            LIMIT 1
            """,
            (
                order_id,
            )
        )



        if fulfillment:


            fulfillment_id = (
                fulfillment["fulfillment_id"]
            )


        else:


            fulfillment_id = _generate_id(
                "FUL"
            )


            db.execute(
                """
                INSERT INTO outbound_fulfillment
                (
                    fulfillment_id,
                    order_id,
                    warehouse_id,
                    status,
                    correlation_id
                )
                VALUES
                (
                    %s,%s,%s,%s,%s
                )
                """,
                (
                    fulfillment_id,
                    order_id,
                    warehouse_id,
                    "READY",
                    correlation_id
                )
            )



        # ====================================================
        # CHECK DUPLICATE SHIPMENT
        # ====================================================

        existing = db.fetch_one(
            """
            SELECT shipment_id
            FROM outbound_shipments
            WHERE package_id=%s
            LIMIT 1
            """,
            (
                package_id,
            )
        )


        if existing:

            raise Exception(
                f"Shipment already exists "
                f"{existing['shipment_id']}"
            )



        # ====================================================
        # CREATE SHIPMENT
        # ====================================================

        shipment_id = _generate_id(
            "SHP"
        )


        shipment_time = _ensure_utc(
            get_simulation_now()
        )


        expected_delivery = (
            shipment_time
            +
            timedelta(
                days=random.randint(
                    1,
                    5
                )
            )
        )



        db.execute(
            """
            INSERT INTO outbound_shipments
            (
                shipment_id,
                fulfillment_id,
                order_id,
                package_id,
                shipment_status,
                shipment_date,
                expected_delivery,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                shipment_id,
                fulfillment_id,
                order_id,
                package_id,
                "READY",
                shipment_time,
                expected_delivery,
                correlation_id
            )
        )



        # ====================================================
        # UPDATE FULFILLMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_fulfillment
            SET
                status='READY'
            WHERE fulfillment_id=%s
            """,
            (
                fulfillment_id,
            )
        )



        # ====================================================
        # UPDATE ORDER
        # ====================================================

        db.execute(
            """
            UPDATE orders
            SET
                order_status='SHIPPED'
            WHERE order_id=%s
            """,
            (
                order_id,
            )
        )



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================

        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                shipment_time.isoformat(),



            "shipment":
            {

                "shipment_id":
                    shipment_id,


                "package_id":
                    package_id,


                "order_id":
                    order_id,


                "fulfillment_id":
                    fulfillment_id,


                "warehouse_id":
                    warehouse_id,


                "status":
                    "READY",


                "expected_delivery":
                    expected_delivery.isoformat()

            },


            "correlation_id":
                correlation_id
        }



        # ====================================================
        # OUTBOX
        # ====================================================

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="SHIPMENT",
            aggregate_id=shipment_id,
            correlation_id=correlation_id,
            payload=payload
        )



        # ====================================================
        # LOG
        # ====================================================

        log_event_success(
            EVENT_NAME,
            {
                "shipment_id": shipment_id,
                "package_id": package_id,
                "order_id": order_id
            }
        )



        print(
            f"""
============================================================
SHIPMENT READY

SHIPMENT:
{shipment_id}

PACKAGE:
{package_id}

ORDER:
{order_id}

STATUS:
READY

============================================================
"""
        )


        return {

            "shipment_id":
                shipment_id,

            "status":
                "READY"

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_shipment_ready()


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise