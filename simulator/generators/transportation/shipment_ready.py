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

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "ShipmentReady"


# ============================================================
# ID GENERATORS
# ============================================================

def _generate_fulfillment_id():
    """
    Generate a unique outbound fulfillment identifier.

    The current database model has one fulfillment per order,
    so this function is used only when the first package of
    an order creates the fulfillment.
    """

    return (
        "FUL-" +
        uuid.uuid4().hex[:12].upper()
    )


def _generate_outbound_shipment_id():
    """
    Generate a unique outbound shipment identifier.
    """

    return (
        "OUTSHIP-" +
        uuid.uuid4().hex[:12].upper()
    )


# ============================================================
# SHIPMENT READY TIMESTAMP
# ============================================================

def _get_shipment_ready_time(
    package
):
    """
    Calculate a causally valid ShipmentReady timestamp.

    ShipmentReady must occur after PackingCompleted.

    Primary predecessor:
        packed_at

    Fallback:
        simulation clock
    """

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
        # 1. FIND EXACT PACKED PACKAGE
        # ====================================================

        if package_id:

            package = db.fetch_one(
                """
                SELECT
                    package_id,
                    order_id,
                    warehouse_id,
                    correlation_id,
                    package_status,
                    packed_at

                FROM packages

                WHERE package_id=%s

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
                    package_id,
                    order_id,
                    warehouse_id,
                    correlation_id,
                    package_status,
                    packed_at

                FROM packages

                WHERE package_status='PACKED'

                ORDER BY packed_at DESC,
                         package_id DESC

                LIMIT 1

                FOR UPDATE
                """
            )

        if not package:

            if package_id:

                raise Exception(
                    f"No package found "
                    f"for package_id={package_id}"
                )

            raise Exception(
                "No package found"
            )

        # ====================================================
        # 2. EXTRACT PACKAGE DETAILS
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

        package_correlation_id = (
            package[
                "correlation_id"
            ]
        )

        package_status = package[
            "package_status"
        ]

        if not order_id:

            raise Exception(
                f"Package {package_id} "
                "has no order_id"
            )

        if not warehouse_id:

            raise Exception(
                f"Package {package_id} "
                "has no warehouse_id"
            )

        # ====================================================
        # 3. CHECK FOR EXISTING SHIPMENT FOR THIS PACKAGE
        #
        # This check is intentionally performed before the
        # order-status validation.
        #
        # That makes ShipmentReady idempotent.
        #
        # If the same package already has a shipment, simply
        # return the existing shipment rather than trying to
        # create another one.
        # ====================================================

        existing_shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                fulfillment_id,
                shipment_status,
                shipment_date,
                expected_delivery

            FROM outbound_shipments

            WHERE package_id=%s

            LIMIT 1

            FOR UPDATE
            """,
            (
                package_id,
            )
        )

        if existing_shipment:

            shipment_id = existing_shipment[
                "shipment_id"
            ]

            fulfillment_id = existing_shipment[
                "fulfillment_id"
            ]

            existing_status = existing_shipment[
                "shipment_status"
            ]

            correlation_id = (
                str(package_correlation_id)
                if package_correlation_id
                else str(uuid.uuid4())
            )

            allowed_existing_states = (
                "READY",
                "ASSIGNED",
                "PICKED_UP",
                "IN_TRANSIT",
                "DELIVERED"
            )

            if existing_status not in (
                allowed_existing_states
            ):

                raise Exception(
                    f"""
Outbound shipment already exists with
an unsupported state.

PACKAGE:
{package_id}

SHIPMENT:
{shipment_id}

STATUS:
{existing_status}
"""
                )

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
                        existing_shipment[
                            "shipment_date"
                        ],

                    "expected_delivery":
                        existing_shipment[
                            "expected_delivery"
                        ],

                    "status":
                        existing_status,

                    "correlation_id":
                        correlation_id
                }
            )

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
                    existing_shipment[
                        "shipment_date"
                    ],

                "expected_delivery":
                    existing_shipment[
                        "expected_delivery"
                    ],

                "status":
                    existing_status
            }

        # ====================================================
        # 4. PACKAGE MUST BE PACKED FOR A NEW SHIPMENT
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
                customer_id,
                warehouse_id,
                promised_delivery_date,
                correlation_id,
                order_status

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
                f"Order not found: {order_id}"
            )

        # ====================================================
        # 6. VALIDATE PACKAGE / ORDER WAREHOUSE
        # ====================================================

        if (
            order["warehouse_id"]
            !=
            warehouse_id
        ):

            raise Exception(
                f"""
Package/order warehouse mismatch.

PACKAGE:
{package_id}

PACKAGE WAREHOUSE:
{warehouse_id}

ORDER:
{order_id}

ORDER WAREHOUSE:
{order["warehouse_id"]}
"""
            )

        # ====================================================
        # 7. VALIDATE ORDER STATUS
        #
        # Valid multi-package states:
        #
        # PACKED
        #     ↓
        # PARTIALLY_DELIVERED
        #     ↓
        # DELIVERED
        #
        # PARTIALLY_DELIVERED allows remaining packages to
        # continue through ShipmentReady.
        # ====================================================

        allowed_order_states = (
            "PACKED",
            "PARTIALLY_DELIVERED"
        )

        if order["order_status"] not in (
            allowed_order_states
        ):

            raise Exception(
                f"""
Order is not eligible for ShipmentReady.

ORDER:
{order_id}

STATUS:
{order["order_status"]}

ALLOWED STATES:
{", ".join(allowed_order_states)}
"""
            )

        # ====================================================
        # 8. DETERMINE CORRELATION ID
        # ====================================================

        correlation_id = (
            str(package_correlation_id)
            if package_correlation_id
            else (
                str(order["correlation_id"])
                if order["correlation_id"]
                else str(uuid.uuid4())
            )
        )

        # ====================================================
        # 9. FIND EXISTING FULFILLMENT FOR ORDER
        #
        # IMPORTANT:
        #
        # Your schema has:
        #
        #     UNIQUE (order_id)
        #
        # on outbound_fulfillment.
        #
        # Therefore there must be ONE fulfillment per order.
        #
        # Multiple packages are represented by multiple
        # outbound_shipments under the same fulfillment.
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

        # ====================================================
        # 10. REUSE EXISTING FULFILLMENT
        # ====================================================

        if fulfillment:

            fulfillment_id = fulfillment[
                "fulfillment_id"
            ]

            fulfillment_status = fulfillment[
                "status"
            ]

            # ------------------------------------------------
            # If the fulfillment is already completed while
            # the package has no shipment, the state is
            # inconsistent and should not silently continue.
            # ------------------------------------------------

            if fulfillment_status == "COMPLETED":

                raise Exception(
                    f"""
Outbound fulfillment is already COMPLETED,
but this package has no outbound shipment.

ORDER:
{order_id}

PACKAGE:
{package_id}

FULFILLMENT:
{fulfillment_id}
"""
                )

            # ------------------------------------------------
            # Keep the existing fulfillment.
            # ------------------------------------------------

            fulfillment_created = False

        # ====================================================
        # 11. CREATE FULFILLMENT FOR FIRST PACKAGE
        # ====================================================

        else:

            fulfillment_id = (
                _generate_fulfillment_id()
            )

            fulfillment_created_at = (
                package["packed_at"]
                or
                get_simulation_now()
            )

            db.execute(
                """
                INSERT INTO outbound_fulfillment
                (
                    fulfillment_id,
                    order_id,
                    warehouse_id,
                    status,
                    created_at,
                    correlation_id
                )

                VALUES
                (
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
                    fulfillment_created_at,
                    correlation_id
                )
            )

            fulfillment_created = True

        # ====================================================
        # 12. VERIFY FULFILLMENT WAREHOUSE
        # ====================================================

        if fulfillment:

            if (
                fulfillment["warehouse_id"]
                !=
                warehouse_id
            ):

                raise Exception(
                    f"""
Fulfillment warehouse mismatch.

ORDER:
{order_id}

PACKAGE:
{package_id}

PACKAGE WAREHOUSE:
{warehouse_id}

FULFILLMENT:
{fulfillment_id}

FULFILLMENT WAREHOUSE:
{fulfillment["warehouse_id"]}
"""
                )

        # ====================================================
        # 13. CALCULATE SHIPMENT READY TIME
        # ====================================================

        ready_at = _get_shipment_ready_time(
            package
        )

        # ====================================================
        # 14. PROMISED DELIVERY
        # ====================================================

        promised_delivery = (
            order[
                "promised_delivery_date"
            ]
        )

        if promised_delivery is None:

            raise Exception(
                f"""
Order has no promised delivery date.

ORDER:
{order_id}
"""
            )

        # ====================================================
        # 15. GENERATE OUTBOUND SHIPMENT ID
        # ====================================================

        shipment_id = (
            _generate_outbound_shipment_id()
        )

        # ====================================================
        # 16. CREATE OUTBOUND SHIPMENT
        #
        # One package -> one shipment.
        #
        # Multiple shipments may share the same fulfillment.
        # ====================================================

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
                created_at,
                updated_at,
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
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                shipment_id,
                fulfillment_id,
                order_id,
                package_id,
                "READY",
                ready_at,
                promised_delivery,
                ready_at,
                ready_at,
                correlation_id
            )
        )

        # ====================================================
        # 17. KEEP FULFILLMENT READY
        #
        # ShipmentReady does not complete the fulfillment.
        # ShipmentDelivered determines when fulfillment is
        # finally completed.
        # ====================================================

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

        # ====================================================
        # 18. EVENT PAYLOAD
        # ====================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                ready_at.isoformat(),

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

                "shipmentStatus":
                    "READY",

                "shipmentDate":
                    ready_at.isoformat(),

                "expectedDelivery":
                    promised_delivery.isoformat()

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
        # 19. PUBLISH EVENT
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
        # 20. LOG EVENT
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
                    ready_at,

                "expected_delivery":
                    promised_delivery,

                "status":
                    "READY",

                "fulfillment_created":
                    fulfillment_created,

                "correlation_id":
                    correlation_id

            }
        )

        # ====================================================
        # 21. RETURN RESULT
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
                ready_at,

            "expected_delivery":
                promised_delivery,

            "status":
                "READY"

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