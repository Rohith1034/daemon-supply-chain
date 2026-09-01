from datetime import timedelta, timezone
import random
import sys

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "ShipmentDelivered"


# ============================================================
# DATETIME NORMALIZATION
# ============================================================

def _ensure_utc(value):
    """
    Normalize a datetime into timezone-aware UTC.

    PostgreSQL may return either naive or timezone-aware
    datetime values depending on the column definition.
    """

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )


# ============================================================
# DELIVERY TIMESTAMP
# ============================================================

def _get_delivery_time(
    in_transit_at,
    expected_delivery
):
    """
    Generate a causally valid delivery timestamp.

    ShipmentDelivered must always happen after
    ShipmentInTransit.

    Delivery scenarios:

        EARLY
        ON_TIME
        LATE

    The generated timestamp is always forced to be
    later than ShipmentInTransit.
    """

    in_transit_at = _ensure_utc(
        in_transit_at
    )

    expected_delivery = _ensure_utc(
        expected_delivery
    )

    if in_transit_at is None:
        in_transit_at = _ensure_utc(
            get_simulation_now()
        )

    # ========================================================
    # PROMISED DELIVERY EXISTS
    # ========================================================

    if expected_delivery is not None:

        scenario = random.random()

        # ----------------------------------------------------
        # EARLY
        # ----------------------------------------------------

        if scenario < 0.20:

            candidate_delivery = (
                expected_delivery
                -
                timedelta(
                    hours=random.randint(
                        3,
                        12
                    )
                )
            )

        # ----------------------------------------------------
        # ON TIME
        # ----------------------------------------------------

        elif scenario < 0.80:

            candidate_delivery = (
                expected_delivery
                +
                timedelta(
                    hours=random.randint(
                        -2,
                        2
                    )
                )
            )

        # ----------------------------------------------------
        # LATE
        # ----------------------------------------------------

        else:

            candidate_delivery = (
                expected_delivery
                +
                timedelta(
                    hours=random.randint(
                        3,
                        24
                    )
                )
            )

        # ----------------------------------------------------
        # Delivery must always occur after IN_TRANSIT.
        # ----------------------------------------------------

        minimum_delivery_time = (
            in_transit_at
            +
            timedelta(
                minutes=30
            )
        )

        return max(
            candidate_delivery,
            minimum_delivery_time
        )

    # ========================================================
    # NO PROMISED DELIVERY
    # ========================================================

    return (
        in_transit_at
        +
        timedelta(
            days=random.randint(
                1,
                3
            ),
            hours=random.randint(
                0,
                12
            )
        )
    )


# ============================================================
# DELIVERY PERFORMANCE
# ============================================================

def _get_delivery_performance(
    delivered_at,
    expected_delivery
):
    """
    Classify delivery performance.

    EARLY:
        More than 2 hours before promise.

    ON_TIME:
        Within ±2 hours of promise.

    LATE:
        More than 2 hours after promise.

    UNKNOWN:
        No promised delivery timestamp exists.
    """

    delivered_at = _ensure_utc(
        delivered_at
    )

    expected_delivery = _ensure_utc(
        expected_delivery
    )

    if expected_delivery is None:
        return "UNKNOWN"

    tolerance = timedelta(
        hours=2
    )

    early_boundary = (
        expected_delivery
        -
        tolerance
    )

    late_boundary = (
        expected_delivery
        +
        tolerance
    )

    if delivered_at < early_boundary:
        return "EARLY"

    if delivered_at <= late_boundary:
        return "ON_TIME"

    return "LATE"


# ============================================================
# ORDER PACKAGE / SHIPMENT PROGRESS
# ============================================================

def _get_order_delivery_progress(
    db,
    order_id
):
    """
    Determine shipment progress using PACKAGES as the
    authoritative total.

    This is critical for split orders.

    Example:

        ORDER
          ├── PKG-001 -> shipment delivered
          ├── PKG-002 -> shipment delivered
          ├── PKG-003 -> shipment exists
          └── PKG-004 -> shipment not created yet

    Total packages = 4

    Delivered shipments = 2

    Remaining packages = 2

    The order is therefore NOT complete.

    The important rule is:

        total packages belong to the order
        delivered shipments represent completed packages
    """

    row = db.fetch_one(
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM packages
                WHERE order_id=%s
            ) AS total_packages,

            (
                SELECT COUNT(*)
                FROM outbound_shipments
                WHERE order_id=%s
                  AND shipment_status='DELIVERED'
            ) AS delivered_shipments
        """,
        (
            order_id,
            order_id
        )
    )

    if not row:
        return {
            "total_packages": 0,
            "delivered_shipments": 0,
            "remaining_packages": 0
        }

    total_packages = int(
        row["total_packages"]
        or 0
    )

    delivered_shipments = int(
        row["delivered_shipments"]
        or 0
    )

    remaining_packages = max(
        total_packages
        -
        delivered_shipments,
        0
    )

    return {
        "total_packages": total_packages,
        "delivered_shipments": delivered_shipments,
        "remaining_packages": remaining_packages
    }


# ============================================================
# MAIN GENERATOR
# ============================================================

def generate_shipment_delivered(
    shipment_id=None
):

    with Database() as db:

        # ====================================================
        # 1. FIND IN-TRANSIT OUTBOUND SHIPMENT
        # ====================================================

        if shipment_id:

            shipment = db.fetch_one(
                """
                SELECT
                    os.shipment_id,
                    os.fulfillment_id,
                    os.order_id,
                    os.package_id,

                    obf.warehouse_id,

                    os.shipment_status,
                    os.shipment_date,
                    os.expected_delivery,
                    os.actual_delivery,
                    os.created_at,
                    os.updated_at,
                    os.correlation_id,

                    ost.id AS transportation_id,
                    ost.vehicle_id,
                    ost.trailer_id,
                    ost.driver_id,
                    ost.assigned_at,
                    ost.picked_up_at,
                    ost.status AS transportation_status

                FROM outbound_shipments os

                INNER JOIN outbound_fulfillment obf
                    ON obf.fulfillment_id =
                       os.fulfillment_id

                INNER JOIN outbound_shipment_transportation ost
                    ON ost.shipment_id =
                       os.shipment_id

                WHERE os.shipment_id=%s
                  AND os.shipment_status='IN_TRANSIT'
                  AND ost.status='IN_TRANSIT'

                LIMIT 1

                FOR UPDATE OF os, ost
                """,
                (
                    shipment_id,
                )
            )

        else:

            shipment = db.fetch_one(
                """
                SELECT
                    os.shipment_id,
                    os.fulfillment_id,
                    os.order_id,
                    os.package_id,

                    obf.warehouse_id,

                    os.shipment_status,
                    os.shipment_date,
                    os.expected_delivery,
                    os.actual_delivery,
                    os.created_at,
                    os.updated_at,
                    os.correlation_id,

                    ost.id AS transportation_id,
                    ost.vehicle_id,
                    ost.trailer_id,
                    ost.driver_id,
                    ost.assigned_at,
                    ost.picked_up_at,
                    ost.status AS transportation_status

                FROM outbound_shipments os

                INNER JOIN outbound_fulfillment obf
                    ON obf.fulfillment_id =
                       os.fulfillment_id

                INNER JOIN outbound_shipment_transportation ost
                    ON ost.shipment_id =
                       os.shipment_id

                WHERE os.shipment_status='IN_TRANSIT'
                  AND ost.status='IN_TRANSIT'

                ORDER BY ost.picked_up_at ASC

                LIMIT 1

                FOR UPDATE OF os, ost
                """
            )

        # ====================================================
        # 2. VERIFY SHIPMENT
        # ====================================================

        if not shipment:

            if shipment_id:

                raise Exception(
                    f"No IN_TRANSIT outbound shipment found "
                    f"for shipment_id={shipment_id}"
                )

            raise Exception(
                "No IN_TRANSIT outbound shipment found"
            )

        # ====================================================
        # 3. EXTRACT DETAILS
        # ====================================================

        shipment_id = shipment[
            "shipment_id"
        ]

        fulfillment_id = shipment[
            "fulfillment_id"
        ]

        order_id = shipment[
            "order_id"
        ]

        package_id = shipment[
            "package_id"
        ]

        warehouse_id = shipment[
            "warehouse_id"
        ]

        vehicle_id = shipment[
            "vehicle_id"
        ]

        trailer_id = shipment[
            "trailer_id"
        ]

        driver_id = shipment[
            "driver_id"
        ]

        transportation_id = shipment[
            "transportation_id"
        ]

        correlation_id = str(
            shipment[
                "correlation_id"
            ]
        )

        # ====================================================
        # 4. VALIDATE REQUIRED DATA
        # ====================================================

        required_values = {

            "fulfillment_id":
                fulfillment_id,

            "warehouse_id":
                warehouse_id,

            "order_id":
                order_id,

            "package_id":
                package_id,

            "vehicle_id":
                vehicle_id,

            "trailer_id":
                trailer_id,

            "driver_id":
                driver_id,

            "transportation_id":
                transportation_id
        }

        for (
            field_name,
            field_value
        ) in required_values.items():

            if not field_value:

                raise Exception(
                    f"Missing {field_name} for "
                    f"shipment {shipment_id}"
                )

        # ====================================================
        # 5. VALIDATE TRANSPORTATION STATE
        # ====================================================

        transportation_status = (
            shipment[
                "transportation_status"
            ]
        )

        if transportation_status != "IN_TRANSIT":

            raise Exception(
                f"""
Transportation is not IN_TRANSIT.

SHIPMENT: {shipment_id}
STATUS: {transportation_status}
"""
            )

        # ====================================================
        # 6. FETCH EXISTING TRACKING RECORD
        #
        # ShipmentPickedUp creates the row.
        # ShipmentInTransit changes it to IN_TRANSIT.
        # ShipmentDelivered changes the same row to DELIVERED.
        # ====================================================

        tracking = db.fetch_one(
            """
            SELECT
                tracking_id,
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                status,
                latitude,
                longitude,
                departure_time,
                estimated_arrival,
                actual_arrival,
                created_at,
                correlation_id

            FROM outbound_shipment_tracking

            WHERE shipment_id=%s
              AND status='IN_TRANSIT'

            ORDER BY created_at DESC

            LIMIT 1

            FOR UPDATE
            """,
            (
                shipment_id,
            )
        )

        if not tracking:

            raise Exception(
                f"""
IN_TRANSIT tracking record not found.

SHIPMENT: {shipment_id}

Expected lifecycle:

ShipmentPickedUp
        ↓ PICKED_UP

ShipmentInTransit
        ↓ IN_TRANSIT

ShipmentDelivered
        ↓ DELIVERED
"""
            )

        tracking_id = tracking[
            "tracking_id"
        ]

        # ====================================================
        # 7. VALIDATE RESOURCE MAPPING
        # ====================================================

        tracking_resources = [

            (
                "vehicle",
                tracking[
                    "vehicle_id"
                ],
                vehicle_id
            ),

            (
                "trailer",
                tracking[
                    "trailer_id"
                ],
                trailer_id
            ),

            (
                "driver",
                tracking[
                    "driver_id"
                ],
                driver_id
            )
        ]

        for (
            resource_name,
            tracking_value,
            shipment_value
        ) in tracking_resources:

            if (
                tracking_value is not None
                and
                tracking_value != shipment_value
            ):

                raise Exception(
                    f"""
Tracking {resource_name} mismatch.

SHIPMENT: {shipment_id}

TRANSPORTATION: {shipment_value}

TRACKING: {tracking_value}
"""
                )

        # ====================================================
        # 8. DETERMINE IN-TRANSIT TIMESTAMP
        #
        # ShipmentInTransit updates outbound_shipments.updated_at
        # to its event timestamp.
        #
        # Therefore updated_at is preferred.
        #
        # tracking.created_at is only a fallback because the
        # tracking row was originally created by ShipmentPickedUp.
        # ====================================================

        in_transit_at = _ensure_utc(
            shipment[
                "updated_at"
            ]
        )

        if in_transit_at is None:

            in_transit_at = _ensure_utc(
                tracking[
                    "created_at"
                ]
            )

        if in_transit_at is None:

            in_transit_at = _ensure_utc(
                tracking[
                    "departure_time"
                ]
            )

        if in_transit_at is None:

            in_transit_at = _ensure_utc(
                shipment[
                    "picked_up_at"
                ]
            )

        if in_transit_at is None:

            raise Exception(
                f"""
Unable to determine IN_TRANSIT timestamp.

SHIPMENT: {shipment_id}
"""
            )

        # ====================================================
        # 9. EXPECTED DELIVERY
        # ====================================================

        expected_delivery = _ensure_utc(
            shipment[
                "expected_delivery"
            ]
        )

        # ====================================================
        # 10. GENERATE DELIVERY TIMESTAMP
        # ====================================================

        delivered_at = _ensure_utc(
            _get_delivery_time(
                in_transit_at,
                expected_delivery
            )
        )

        # ====================================================
        # 11. TIMELINE VALIDATION
        # ====================================================

        if delivered_at <= in_transit_at:

            raise Exception(
                f"""
Invalid delivery timeline.

SHIPMENT: {shipment_id}

IN TRANSIT: {in_transit_at}

DELIVERED: {delivered_at}
"""
            )

        # ====================================================
        # 12. DELIVERY PERFORMANCE
        # ====================================================

        delivery_performance = (
            _get_delivery_performance(
                delivered_at,
                expected_delivery
            )
        )

        # ====================================================
        # 13. UPDATE OUTBOUND SHIPMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipments

            SET
                shipment_status='DELIVERED',
                actual_delivery=%s,
                updated_at=%s

            WHERE shipment_id=%s
              AND shipment_status='IN_TRANSIT'
            """,
            (
                delivered_at,
                delivered_at,
                shipment_id
            )
        )

        # ====================================================
        # 14. UPDATE TRANSPORTATION
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_transportation

            SET
                status='DELIVERED',
                updated_at=%s

            WHERE id=%s
              AND shipment_id=%s
              AND status='IN_TRANSIT'
            """,
            (
                delivered_at,
                transportation_id,
                shipment_id
            )
        )

        # ====================================================
        # 15. UPDATE EXISTING TRACKING ROW
        #
        # IMPORTANT:
        #
        # Do NOT INSERT another tracking row.
        #
        # TRACK-XXXX remains the same throughout:
        #
        # PICKED_UP
        #      ↓
        # IN_TRANSIT
        #      ↓
        # DELIVERED
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_tracking

            SET
                status='DELIVERED',
                actual_arrival=%s

            WHERE tracking_id=%s
              AND shipment_id=%s
              AND status='IN_TRANSIT'
            """,
            (
                delivered_at,
                tracking_id,
                shipment_id
            )
        )

        # ====================================================
        # 16. RELEASE VEHICLE
        # ====================================================

        db.execute(
            """
            UPDATE vehicles

            SET
                status='ACTIVE'

            WHERE vehicle_id=%s
            """,
            (
                vehicle_id,
            )
        )

        # ====================================================
        # 17. RELEASE TRAILER
        # ====================================================

        db.execute(
            """
            UPDATE trailers

            SET
                status='ACTIVE'

            WHERE trailer_id=%s
            """,
            (
                trailer_id,
            )
        )

        # ====================================================
        # 18. RELEASE DRIVER
        # ====================================================

        db.execute(
            """
            UPDATE drivers

            SET
                status='ACTIVE'

            WHERE driver_id=%s
            """,
            (
                driver_id,
            )
        )

        # ====================================================
        # 19. DETERMINE ORDER DELIVERY PROGRESS
        #
        # IMPORTANT:
        #
        # Total is based on packages, not outbound shipments.
        #
        # This prevents:
        #
        #   4 packages
        #   1 shipment created
        #   1 shipment delivered
        #
        # from incorrectly producing:
        #
        #   ORDER = DELIVERED
        # ====================================================

        progress = (
            _get_order_delivery_progress(
                db,
                order_id
            )
        )

        total_packages = (
            progress[
                "total_packages"
            ]
        )

        delivered_shipments = (
            progress[
                "delivered_shipments"
            ]
        )

        remaining_packages = (
            progress[
                "remaining_packages"
            ]
        )

        if total_packages <= 0:

            raise Exception(
                f"""
Order contains no packages.

ORDER: {order_id}

Cannot determine delivery completion.
"""
            )

        # ====================================================
        # 20. DETERMINE ORDER STATUS
        # ====================================================

        if (
            remaining_packages == 0
            and
            delivered_shipments >= total_packages
        ):

            order_status = (
                "DELIVERED"
            )

            fulfillment_status = (
                "COMPLETED"
            )

        else:

            order_status = (
                "PARTIALLY_DELIVERED"
            )

            fulfillment_status = (
                "PROCESSING"
            )

        # ====================================================
        # 21. UPDATE ORDER
        # ====================================================

        db.execute(
            """
            UPDATE orders

            SET
                order_status=%s

            WHERE order_id=%s
            """,
            (
                order_status,
                order_id
            )
        )

        # ====================================================
        # 22. UPDATE OUTBOUND FULFILLMENT
        #
        # ONE ORDER -> ONE FULFILLMENT
        #
        # Fulfillment is completed only after every package
        # belonging to the order has been delivered.
        # ====================================================

        if fulfillment_status == "COMPLETED":

            db.execute(
                """
                UPDATE outbound_fulfillment

                SET
                    status='COMPLETED',
                    completed_at=%s

                WHERE fulfillment_id=%s
                """,
                (
                    delivered_at,
                    fulfillment_id
                )
            )

        else:

            db.execute(
                """
                UPDATE outbound_fulfillment

                SET
                    status='PROCESSING',
                    completed_at=NULL

                WHERE fulfillment_id=%s
                """,
                (
                    fulfillment_id,
                )
            )

        # ====================================================
        # 23. EVENT PAYLOAD
        # ====================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                delivered_at.isoformat(),

            "shipment": {

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

                "vehicleId":
                    vehicle_id,

                "trailerId":
                    trailer_id,

                "driverId":
                    driver_id,

                "trackingId":
                    tracking_id,

                "status":
                    "DELIVERED",

                "inTransitAt":
                    in_transit_at.isoformat(),

                "deliveredAt":
                    delivered_at.isoformat(),

                "expectedDelivery":
                    (
                        expected_delivery.isoformat()
                        if expected_delivery
                        else None
                    ),

                "deliveryPerformance":
                    delivery_performance
            },

            "transportation": {

                "transportationId":
                    transportation_id,

                "status":
                    "DELIVERED",

                "vehicleStatus":
                    "ACTIVE",

                "trailerStatus":
                    "ACTIVE",

                "driverStatus":
                    "ACTIVE"
            },

            "tracking": {

                "trackingId":
                    tracking_id,

                "status":
                    "DELIVERED",

                "departureTime":
                    (
                        _ensure_utc(
                            tracking[
                                "departure_time"
                            ]
                        ).isoformat()
                        if tracking[
                            "departure_time"
                        ]
                        else None
                    ),

                "inTransitAt":
                    in_transit_at.isoformat(),

                "actualArrival":
                    delivered_at.isoformat()
            },

            "order": {

                "orderId":
                    order_id,

                "orderStatus":
                    order_status,

                "totalPackages":
                    total_packages,

                "deliveredShipments":
                    delivered_shipments,

                "remainingPackages":
                    remaining_packages
            },

            "fulfillment": {

                "fulfillmentId":
                    fulfillment_id,

                "status":
                    fulfillment_status
            },

            "correlationId":
                correlation_id
        }

        # ====================================================
        # 24. PUBLISH OUTBOX EVENT
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
        # 25. LOG EVENT
        # ====================================================

        log_event_success(
            EVENT_NAME,
            {

                "shipment_id":
                    shipment_id,

                "fulfillment_id":
                    fulfillment_id,

                "order_id":
                    order_id,

                "package_id":
                    package_id,

                "warehouse_id":
                    warehouse_id,

                "vehicle_id":
                    vehicle_id,

                "trailer_id":
                    trailer_id,

                "driver_id":
                    driver_id,

                "tracking_id":
                    tracking_id,

                "in_transit_at":
                    in_transit_at,

                "delivered_at":
                    delivered_at,

                "expected_delivery":
                    expected_delivery,

                "delivery_performance":
                    delivery_performance,

                "order_status":
                    order_status,

                "fulfillment_status":
                    fulfillment_status,

                "total_packages":
                    total_packages,

                "delivered_shipments":
                    delivered_shipments,

                "remaining_packages":
                    remaining_packages,

                "status":
                    "DELIVERED",

                "correlation_id":
                    correlation_id
            }
        )

        # ====================================================
        # 26. RETURN RESULT
        # ====================================================

        return {

            "shipment_id":
                shipment_id,

            "fulfillment_id":
                fulfillment_id,

            "order_id":
                order_id,

            "package_id":
                package_id,

            "warehouse_id":
                warehouse_id,

            "tracking_id":
                tracking_id,

            "vehicle_id":
                vehicle_id,

            "trailer_id":
                trailer_id,

            "driver_id":
                driver_id,

            "status":
                "DELIVERED",

            "order_status":
                order_status,

            "fulfillment_status":
                fulfillment_status,

            "in_transit_at":
                in_transit_at,

            "delivered_at":
                delivered_at,

            "expected_delivery":
                expected_delivery,

            "delivery_performance":
                delivery_performance,

            "total_packages":
                total_packages,

            "delivered_shipments":
                delivered_shipments,

            "remaining_packages":
                remaining_packages
        }


# ============================================================
# CLI ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        if len(sys.argv) > 1:

            generate_shipment_delivered(
                sys.argv[1]
            )

        else:

            generate_shipment_delivered()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise