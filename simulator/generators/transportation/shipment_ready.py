from datetime import timedelta
import random
import uuid
import sys

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "ShipmentReady"


# ============================================================
# ID GENERATORS
# ============================================================

def _generate_fulfillment_id():

    return (
        "FUL-" +
        uuid.uuid4().hex[:12].upper()
    )


def _generate_shipment_id():

    return (
        "OUTSHIP-" +
        uuid.uuid4().hex[:12].upper()
    )


# ============================================================
# SHIPMENT READY TIME
# ============================================================

def _get_shipment_ready_time(package):

    packed_at = package.get(
        "packed_at"
    )

    if packed_at is None:

        packed_at = get_simulation_now()

    return (
        packed_at +
        timedelta(
            minutes=random.randint(
                15,
                60
            )
        )
    )


# ============================================================
# MAIN GENERATOR
# ============================================================

def generate_shipment_ready(
    package_id=None
):

    with Database() as db:


        # ====================================================
        # 1. FETCH PACKED PACKAGE
        #
        # IMPORTANT FIX:
        #
        # Do not select packages which already have shipment.
        #
        # Earlier issue:
        #
        # PKG-000000004 remained PACKED
        # but shipment became DELIVERED.
        #
        # Second run selected same package again.
        # ====================================================

        if package_id:

            package = db.fetch_one(
                """
                SELECT
                    p.package_id,
                    p.order_id,
                    p.package_status,
                    p.packed_at,
                    p.correlation_id,
                    o.warehouse_id
                FROM packages p
                JOIN orders o
                    ON p.order_id=o.order_id
                WHERE p.package_id=%s
                LIMIT 1
                FOR UPDATE
                """,
                (
                    package_id,
                )
            )


        else:

            package = db.fetch_one(
                """
                SELECT
                    p.package_id,
                    p.order_id,
                    p.package_status,
                    p.packed_at,
                    p.correlation_id,
                    o.warehouse_id

                FROM packages p

                JOIN orders o
                    ON p.order_id=o.order_id


                WHERE p.package_status='PACKED'


                AND NOT EXISTS
                (
                    SELECT 1
                    FROM outbound_shipments os
                    WHERE os.package_id=p.package_id
                )


                ORDER BY
                    p.packed_at DESC,
                    p.package_id DESC


                LIMIT 1


                FOR UPDATE
                """
            )


        if not package:


            if package_id:

                raise Exception(
                    f"""
Package not found.

PACKAGE:
{package_id}
"""
                )


            raise Exception(
                "No eligible PACKED package found"
            )



        # ====================================================
        # 2. EXTRACT PACKAGE DATA
        # ====================================================

        package_id = package[
            "package_id"
        ]


        order_id = package[
            "order_id"
        ]


        warehouse_id = package[
            "warehouse_id"
        ]


        package_status = package[
            "package_status"
        ]



        # ====================================================
        # 3. EXISTING SHIPMENT CHECK
        #
        # ShipmentReady is idempotent.
        #
        # One package = one shipment.
        # ====================================================

        existing_shipment = db.fetch_one(
            """
            SELECT

                os.shipment_id,
                os.fulfillment_id,
                os.order_id,
                os.package_id,

                of.warehouse_id,

                os.shipment_status,
                os.shipment_date,
                os.expected_delivery,

                os.created_at,
                os.updated_at,

                os.correlation_id


            FROM outbound_shipments os


            JOIN outbound_fulfillment of

                ON os.fulfillment_id =
                   of.fulfillment_id


            WHERE os.package_id=%s


            LIMIT 1


            FOR UPDATE
            """,
            (
                package_id,
            )
        )



        if existing_shipment:


            existing_order_id = (
                existing_shipment[
                    "order_id"
                ]
            )


            if existing_order_id != order_id:

                raise Exception(
                    f"""
Shipment ownership mismatch.

PACKAGE:
{package_id}

PACKAGE ORDER:
{order_id}

SHIPMENT ORDER:
{existing_order_id}
"""
                )



            existing_status = (
                existing_shipment[
                    "shipment_status"
                ]
            )



            # Existing valid lifecycle

            if existing_status in (

                "READY",
                "ASSIGNED",
                "PICKED_UP",
                "IN_TRANSIT",
                "DELIVERED"

            ):


                log_event_success(
                    EVENT_NAME,
                    {

                        "shipment_id":
                            existing_shipment[
                                "shipment_id"
                            ],


                        "fulfillment_id":
                            existing_shipment[
                                "fulfillment_id"
                            ],


                        "package_id":
                            package_id,


                        "order_id":
                            order_id,


                        "status":
                            existing_status,


                        "idempotent":
                            True,


                        "correlation_id":
                            str(
                                existing_shipment[
                                    "correlation_id"
                                ]
                            )
                    }
                )



                return {

                    "shipment_id":
                        existing_shipment[
                            "shipment_id"
                        ],

                    "fulfillment_id":
                        existing_shipment[
                            "fulfillment_id"
                        ],

                    "package_id":
                        package_id,

                    "order_id":
                        order_id,

                    "status":
                        existing_status,

                    "idempotent":
                        True
                }



            raise Exception(
                f"""
Existing shipment has invalid state.

SHIPMENT:
{existing_shipment["shipment_id"]}

STATUS:
{existing_status}
"""
            )



        # ====================================================
        # 4. PACKAGE VALIDATION
        # ====================================================

        if package_status != "PACKED":

            raise Exception(
                f"""
Package is not PACKED.

PACKAGE:
{package_id}

STATUS:
{package_status}
"""
            )


        # ====================================================
        # 5. FETCH ORDER
        # ====================================================

        order = db.fetch_one(
            """
            SELECT
                order_id,
                warehouse_id,
                order_status,
                promised_delivery_date,
                correlation_id

            FROM orders

            WHERE order_id=%s

            LIMIT 1

            FOR UPDATE
            """,
            (
                order_id,
            )
        )


        if not order:

            raise Exception(
                f"""
Order not found.

ORDER:
{order_id}

PACKAGE:
{package_id}
"""
            )



        # ====================================================
        # 6. WAREHOUSE VALIDATION
        # ====================================================

        if (
            order["warehouse_id"]
            != warehouse_id
        ):

            raise Exception(
                f"""
Warehouse mismatch.

ORDER:
{order_id}

ORDER WAREHOUSE:
{order["warehouse_id"]}

PACKAGE WAREHOUSE:
{warehouse_id}
"""
            )



        # ====================================================
        # 7. ORDER STATE VALIDATION
        # ====================================================

        order_status = (
            order["order_status"]
        )


        allowed_order_states = (

            "PACKED",
            "PARTIALLY_DELIVERED"

        )


        if order_status not in allowed_order_states:


            raise Exception(
                f"""
Order is not ready for ShipmentReady.

ORDER:
{order_id}

STATUS:
{order_status}

ALLOWED:
{allowed_order_states}
"""
            )



        # ====================================================
        # 8. CORRELATION ID
        # ====================================================

        correlation_id = (

            str(
                package["correlation_id"]
            )

            if package["correlation_id"]

            else

            str(
                order["correlation_id"]
            )

            if order["correlation_id"]

            else

            str(
                uuid.uuid4()
            )

        )



        # ====================================================
        # 9. FIND FULFILLMENT
        #
        # Business rule:
        #
        # One order -> one fulfillment
        #
        # Multiple packages share same fulfillment.
        # ====================================================

        fulfillment = db.fetch_one(
            """
            SELECT

                fulfillment_id,
                order_id,
                warehouse_id,
                status,
                created_at,
                completed_at,
                correlation_id

            FROM outbound_fulfillment

            WHERE order_id=%s

            LIMIT 1

            FOR UPDATE
            """,
            (
                order_id,
            )
        )



        fulfillment_created = False



        # ====================================================
        # 10. EXISTING FULFILLMENT
        # ====================================================

        if fulfillment:


            fulfillment_id = (
                fulfillment[
                    "fulfillment_id"
                ]
            )


            fulfillment_status = (
                fulfillment[
                    "status"
                ]
            )



            # Ownership protection

            if (
                fulfillment[
                    "order_id"
                ]
                != order_id
            ):

                raise Exception(
                    f"""
Fulfillment ownership mismatch.

ORDER:
{order_id}

FULFILLMENT:
{fulfillment_id}
"""
                )



            # Warehouse protection

            if (
                fulfillment[
                    "warehouse_id"
                ]
                != warehouse_id
            ):

                raise Exception(
                    f"""
Fulfillment warehouse mismatch.

ORDER:
{order_id}

FULFILLMENT:
{fulfillment_id}

FULFILLMENT WAREHOUSE:
{fulfillment["warehouse_id"]}

PACKAGE WAREHOUSE:
{warehouse_id}
"""
                )



            # =================================================
            # IMPORTANT CHANGE
            #
            # Previously:
            #
            # COMPLETED fulfillment caused failure.
            #
            # Now:
            #
            # Check if all packages are already delivered.
            # =================================================

            if fulfillment_status == "COMPLETED":


                package_count = db.fetch_one(
                    """
                    SELECT COUNT(*) AS count

                    FROM packages

                    WHERE order_id=%s
                    """,
                    (
                        order_id,
                    )
                )



                delivered_count = db.fetch_one(
                    """
                    SELECT COUNT(*) AS count

                    FROM outbound_shipments

                    WHERE order_id=%s

                    AND shipment_status='DELIVERED'
                    """,
                    (
                        order_id,
                    )
                )



                if (
                    package_count["count"]
                    ==
                    delivered_count["count"]
                ):

                    raise Exception(
                        f"""
Order already completely fulfilled.

ORDER:
{order_id}

FULFILLMENT:
{fulfillment_id}
"""
                    )


                else:

                    # New package appeared after previous
                    # delivery.
                    #
                    # Continue lifecycle.

                    db.execute(
                        """
                        UPDATE outbound_fulfillment

                        SET
                            status='READY',
                            completed_at=NULL

                        WHERE fulfillment_id=%s
                        """,
                        (
                            fulfillment_id,
                        )
                    )



        else:


            # =================================================
            # 11. CREATE FULFILLMENT
            # =================================================

            fulfillment_id = (
                _generate_fulfillment_id()
            )


            fulfillment_created = True



            db.execute(
                """
                INSERT INTO outbound_fulfillment
                (
                    fulfillment_id,
                    order_id,
                    warehouse_id,
                    status,
                    created_at,
                    completed_at,
                    correlation_id
                )

                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    fulfillment_id,
                    order_id,
                    warehouse_id,
                    "READY",
                    get_simulation_now(),
                    None,
                    correlation_id
                )
            )



        # ====================================================
        # 12. SHIPMENT READY TIMESTAMP
        # ====================================================

        shipment_date = (
            _get_shipment_ready_time(
                package
            )
        )



        # ====================================================
        # 13. EXPECTED DELIVERY
        # ====================================================

        expected_delivery = (
            order[
                "promised_delivery_date"
            ]
        )


        if expected_delivery is None:

            raise Exception(
                f"""
Missing promised delivery date.

ORDER:
{order_id}
"""
            )



        # ====================================================
        # 14. CREATE OUTBOUND SHIPMENT
        # ====================================================

        shipment_id = (
            _generate_shipment_id()
        )



        db.execute(
            """
            INSERT INTO outbound_shipments
            (
                shipment_id,
                fulfillment_id,
                order_id,
                package_id,
                destination_city,
                shipment_status,
                shipment_date,
                expected_delivery,
                actual_delivery,
                created_at,
                updated_at,
                correlation_id
            )

            VALUES
            (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s
            )
            """,
            (
                shipment_id,
                fulfillment_id,
                order_id,
                package_id,
                None,
                "READY",
                shipment_date,
                expected_delivery,
                None,
                shipment_date,
                shipment_date,
                correlation_id
            )
        )



        # ====================================================
        # 15. UPDATE PACKAGE STATUS
        #
        # Critical lifecycle fix.
        #
        # Prevents old PACKED packages being selected
        # again in next simulation run.
        # ====================================================

        db.execute(
            """
            UPDATE packages

            SET
                package_status='READY_FOR_SHIPMENT'

            WHERE package_id=%s
            """,
            (
                package_id,
            )
        )


        # ====================================================
        # 16. FULFILLMENT STATUS PROTECTION
        #
        # IMPORTANT:
        #
        # Do NOT blindly update COMPLETED -> READY.
        #
        # ShipmentReady only moves:
        #
        # CREATED -> READY
        #
        # ====================================================

        db.execute(
            """
            UPDATE outbound_fulfillment

            SET
                status='READY',
                completed_at=NULL

            WHERE fulfillment_id=%s

            AND status IN
            (
                'CREATED',
                'PROCESSING'
            )

            """,
            (
                fulfillment_id,
            )
        )



        # ====================================================
        # 17. EVENT PAYLOAD
        # ====================================================

        payload = {


            "eventType":
                EVENT_NAME,


            "occurredAt":
                shipment_date.isoformat(),



            "shipment":
            {

                "shipmentId":
                    shipment_id,


                "fulfillmentId":
                    fulfillment_id,


                "orderId":
                    order_id,


                "packageId":
                    package_id,


                "warehouseId":
                    warehouse_id,


                "shipmentDate":
                    shipment_date.isoformat(),


                "expectedDelivery":
                    expected_delivery.isoformat(),


                "status":
                    "READY"

            },



            "fulfillment":
            {

                "fulfillmentId":
                    fulfillment_id,


                "orderId":
                    order_id,


                "warehouseId":
                    warehouse_id,


                "status":
                    "READY"

            },


            "correlationId":
                correlation_id

        }



        # ====================================================
        # 18. WRITE OUTBOX EVENT
        # ====================================================

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="OUTBOUND_SHIPMENT",

            aggregate_id=shipment_id,

            correlation_id=correlation_id,

            payload=payload

        )



        # ====================================================
        # 19. SUCCESS LOG
        # ====================================================

        log_event_success(

            EVENT_NAME,

            {

                "shipment_id":
                    shipment_id,


                "fulfillment_id":
                    fulfillment_id,


                "package_id":
                    package_id,


                "order_id":
                    order_id,


                "warehouse_id":
                    warehouse_id,


                "shipment_date":
                    shipment_date,


                "expected_delivery":
                    expected_delivery,


                "status":
                    "READY",


                "fulfillment_created":
                    fulfillment_created,


                "correlation_id":
                    correlation_id

            }

        )



        # ====================================================
        # 20. RETURN
        # ====================================================

        return {


            "shipment_id":
                shipment_id,


            "fulfillment_id":
                fulfillment_id,


            "package_id":
                package_id,


            "order_id":
                order_id,


            "warehouse_id":
                warehouse_id,


            "shipment_date":
                shipment_date,


            "expected_delivery":
                expected_delivery,


            "status":
                "READY",


            "fulfillment_created":
                fulfillment_created,


            "correlation_id":
                correlation_id

        }





# ============================================================
# CLI ENTRY POINT
# ============================================================

if __name__ == "__main__":


    try:


        if len(sys.argv) > 1:


            generate_shipment_ready(
                sys.argv[1]
            )


        else:


            generate_shipment_ready()



    except Exception as e:


        log_event_failure(

            EVENT_NAME,

            e

        )


        raise