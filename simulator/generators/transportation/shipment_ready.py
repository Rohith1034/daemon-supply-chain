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
    ShipmentReady must occur after PackingCompleted.

    packed_at is the preferred predecessor timestamp.

    The simulation clock is used only as a fallback.
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
                  AND package_status='PACKED'

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

                ORDER BY packed_at DESC

                LIMIT 1

                FOR UPDATE
                """
            )

        if not package:

            if package_id:

                raise Exception(
                    f"No PACKED package found "
                    f"for package_id={package_id}"
                )

            raise Exception(
                "No PACKED package found"
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
            package["correlation_id"]
        )

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
        # 3. FETCH ORDER
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
        # 4. VALIDATE PACKAGE / ORDER WAREHOUSE
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
        # 5. VALIDATE ORDER STATUS
        #
        # IMPORTANT:
        #
        # The order may already have one or more packages
        # delivered.
        #
        # Example:
        #
        # Package 1 -> DELIVERED
        # Package 2 -> waiting for ShipmentReady
        # Package 3 -> waiting for ShipmentReady
        #
        # Therefore PARTIALLY_DELIVERED is a valid state.
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
        # 6. FIND EXISTING OUTBOUND FULFILLMENT
        #
        # There should normally be ONE fulfillment per order,
        # even when the order is split across multiple packages.
        # ====================================================

        fulfillment = db.fetch_one(
            """
            SELECT
                fulfillment_id,
                status

            FROM outbound_fulfillment

            WHERE order_id=%s

            ORDER BY created_at ASC

            LIMIT 1

            FOR UPDATE
            """,
            (
                order_id,
            )
        )

        if fulfillment:

            fulfillment_id = (
                fulfillment["fulfillment_id"]
            )

            fulfillment_status = (
                fulfillment["status"]
            )

            # ------------------------------------------------
            # A completed fulfillment cannot create a new
            # shipment.
            #
            # This normally means every package has already
            # been delivered.
            # ------------------------------------------------

            if fulfillment_status == "COMPLETED":

                raise Exception(
                    f"""
Outbound fulfillment is already COMPLETED.

ORDER:
{order_id}

FULFILLMENT:
{fulfillment_id}
"""
                )

        else:

            # =================================================
            # 7. CREATE FULFILLMENT
            # =================================================

            fulfillment_id = (
                _generate_fulfillment_id()
            )

            fulfillment_created_at = (
                package["packed_at"]
                or
                get_simulation_now()
            )

            correlation_id = (
                package_correlation_id
                or
                order["correlation_id"]
                or
                uuid.uuid4()
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
        # 9. CALCULATE SHIPMENT READY TIME
        # ====================================================

        ready_at = _get_shipment_ready_time(
            package
        )

        # ====================================================
        # 10. CHECK FOR EXISTING SHIPMENT FOR THIS PACKAGE
        #
        # Important:
        #
        # Each package gets its own outbound shipment.
        #
        # Therefore this lookup is package-specific.
        # ====================================================

        outbound_shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                fulfillment_id,
                shipment_status

            FROM outbound_shipments

            WHERE package_id=%s

            ORDER BY created_at ASC

            LIMIT 1

            FOR UPDATE
            """,
            (
                package_id,
            )
        )

        # ====================================================
        # 11. EXISTING SHIPMENT
        # ====================================================

        if outbound_shipment:

            shipment_id = (
                outbound_shipment[
                    "shipment_id"
                ]
            )

            existing_status = (
                outbound_shipment[
                    "shipment_status"
                ]
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

            # ------------------------------------------------
            # Keep the existing fulfillment.
            # ------------------------------------------------

            fulfillment_id = (
                outbound_shipment[
                    "fulfillment_id"
                ]
                or
                fulfillment_id
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

                "status":
                    existing_status
            }

        # ====================================================
        # 12. CREATE NEW OUTBOUND SHIPMENT
        # ====================================================

        shipment_id = (
            _generate_outbound_shipment_id()
        )

        # ====================================================
        # 13. PROMISED DELIVERY
        # ====================================================

        promised_delivery = (
            order["promised_delivery_date"]
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
        # 14. INSERT OUTBOUND SHIPMENT
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
        # 15. UPDATE FULFILLMENT
        #
        # Do NOT mark the fulfillment COMPLETED here.
        #
        # It remains READY while package shipments are
        # being created and processed.
        # ====================================================

        db.execute(
            """
            UPDATE outbound_fulfillment

            SET
                status='READY'

            WHERE fulfillment_id=%s

              AND status != 'COMPLETED'
            """,
            (
                fulfillment_id,
            )
        )

        # ====================================================
        # 16. EVENT PAYLOAD
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

            "correlationId":
                correlation_id
        }

        # ====================================================
        # 17. PUBLISH EVENT
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
        # 18. LOG EVENT
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

                "correlation_id":
                    correlation_id
            }
        )

        # ====================================================
        # 19. RETURN
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
