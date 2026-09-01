from datetime import timedelta, timezone
import random
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

    ShipmentDelivered must always occur after
    ShipmentInTransit.

    When expected_delivery exists, the simulator generates:

        EARLY
        ON_TIME
        LATE

    The final timestamp is always forced to occur after
    ShipmentInTransit.
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
    # EXPECTED DELIVERY EXISTS
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
                        1,
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
                        1,
                        24
                    )
                )
            )

        # ----------------------------------------------------
        # Causal protection.
        #
        # Delivery must always happen after IN_TRANSIT.
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
    # NO EXPECTED DELIVERY
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
        More than 2 hours before promised delivery.

    ON_TIME:
        Within ±2 hours of promised delivery.

    LATE:
        More than 2 hours after promised delivery.

    UNKNOWN:
        No promised delivery exists.
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
# ORDER DELIVERY STATUS
# ============================================================

def _update_order_delivery_status(
    db,
    order_id
):
    """
    Determine the final order status based on all outbound
    shipments belonging to the order.

    Rules:

        All shipments DELIVERED
            -> DELIVERED

        Some shipments DELIVERED but others are not
            -> PARTIALLY_DELIVERED

        No shipments DELIVERED
            -> current status remains unchanged
    """

    shipment_statuses = db.fetch_all(
        """
        SELECT
            shipment_status
        FROM outbound_shipments
        WHERE order_id=%s
        """,
        (
            order_id,
        )
    )

    if not shipment_statuses:

        raise Exception(
            f"""
No outbound shipments found while updating
order delivery status.

ORDER:
{order_id}
"""
        )

    total_shipments = len(
        shipment_statuses
    )

    delivered_shipments = sum(
        1
        for row in shipment_statuses
        if row["shipment_status"] == "DELIVERED"
    )

    if delivered_shipments == total_shipments:

        final_status = "DELIVERED"

    elif delivered_shipments > 0:

        final_status = "PARTIALLY_DELIVERED"

    else:

        final_status = None

    if final_status:

        db.execute(
            """
            UPDATE orders
            SET
                order_status=%s
            WHERE order_id=%s
            """,
            (
                final_status,
                order_id
            )
        )

    return {
        "total_shipments":
            total_shipments,

        "delivered_shipments":
            delivered_shipments,

        "remaining_shipments":
            total_shipments -
            delivered_shipments,

        "order_status":
            final_status
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
        #
        # Shipment and transportation are locked here.
        # Tracking is locked separately.
        # ====================================================

        if shipment_id:

            shipment = db.fetch_one(
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

                INNER JOIN outbound_fulfillment of
                    ON of.fulfillment_id =
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

                    of.warehouse_id,

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

                INNER JOIN outbound_fulfillment of
                    ON of.fulfillment_id =
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
        # 2. VERIFY SHIPMENT EXISTS
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
        # 4. REQUIRED FIELD VALIDATION
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

        transportation_status = shipment[
            "transportation_status"
        ]

        if transportation_status != "IN_TRANSIT":

            raise Exception(
                f"""
Transportation is not IN_TRANSIT.

SHIPMENT:
{shipment_id}

STATUS:
{transportation_status}
"""
            )

        # ====================================================
        # 6. FIND EXISTING IN-TRANSIT TRACKING
        #
        # Lifecycle:
        #
        # ShipmentPickedUp
        #     PICKED_UP
        #
        # ShipmentInTransit
        #     IN_TRANSIT
        #
        # ShipmentDelivered
        #     DELIVERED
        #
        # Same tracking_id is reused.
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

SHIPMENT:
{shipment_id}

Expected lifecycle:

ShipmentPickedUp
        ↓
PICKED_UP

ShipmentInTransit
        ↓
IN_TRANSIT

ShipmentDelivered
        ↓
DELIVERED
"""
            )

        tracking_id = tracking[
            "tracking_id"
        ]

        # ====================================================
        # 7. VALIDATE TRACKING RESOURCE MAPPING
        # ====================================================

        tracking_resources = [

            (
                "vehicle",
                tracking["vehicle_id"],
                vehicle_id
            ),

            (
                "trailer",
                tracking["trailer_id"],
                trailer_id
            ),

            (
                "driver",
                tracking["driver_id"],
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

SHIPMENT:
{shipment_id}

TRANSPORTATION:
{shipment_value}

TRACKING:
{tracking_value}
"""
                )

        # ====================================================
        # 8. DETERMINE IN-TRANSIT TIMESTAMP
        #
        # ShipmentInTransit updates outbound_shipments.updated_at
        # to the IN_TRANSIT event timestamp.
        #
        # That is preferred over tracking.created_at because the
        # tracking row was originally created during pickup.
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

SHIPMENT:
{shipment_id}
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
        # 10. GENERATE DELIVERY TIME
        # ====================================================

        delivered_at = _get_delivery_time(
            in_transit_at,
            expected_delivery
        )

        delivered_at = _ensure_utc(
            delivered_at
        )

        # ====================================================
        # 11. FINAL TIMELINE VALIDATION
        # ====================================================

        if delivered_at <= in_transit_at:

            raise Exception(
                f"""
Invalid delivery timeline.

SHIPMENT:
{shipment_id}

IN TRANSIT:
{in_transit_at}

DELIVERED:
{delivered_at}
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
        # 15. UPDATE TRACKING
        #
        # Same tracking record.
        # No new tracking_id.
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
        # 19. DETERMINE ORDER DELIVERY STATUS
        #
        # This is the critical multi-package behavior.
        #
        # Example:
        #
        # Shipment 1 -> DELIVERED
        # Shipment 2 -> IN_TRANSIT
        # Shipment 3 -> READY
        #
        # Order -> PARTIALLY_DELIVERED
        #
        # Once all shipments are DELIVERED:
        #
        # Order -> DELIVERED
        # ====================================================

        order_delivery = (
            _update_order_delivery_status(
                db,
                order_id
            )
        )

        order_status = (
            order_delivery["order_status"]
        )

        # ====================================================
        # 20. COMPLETE FULFILLMENT ONLY WHEN
        #     ALL SHIPMENTS FOR THE ORDER ARE DELIVERED
        # ====================================================

        if (
            order_status
            ==
            "DELIVERED"
        ):

            db.execute(
                """
                UPDATE outbound_fulfillment

                SET
                    status='COMPLETED',

                    completed_at=%s

                WHERE fulfillment_id=%s

                  AND status != 'COMPLETED'
                """,
                (
                    delivered_at,

                    fulfillment_id
                )
            )

        else:

            # ------------------------------------------------
            # Fulfillment remains active while packages are
            # still being transported.
            # ------------------------------------------------

            db.execute(
                """
                UPDATE outbound_fulfillment

                SET
                    status='PROCESSING'

                WHERE fulfillment_id=%s

                  AND status != 'COMPLETED'
                """,
                (
                    fulfillment_id
                )
            )

        # ====================================================
        # 21. EVENT PAYLOAD
        # ====================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                delivered_at.isoformat(),

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

            "order":
            {

                "orderId":
                    order_id,

                "orderStatus":
                    order_status,

                "totalShipments":
                    order_delivery[
                        "total_shipments"
                    ],

                "deliveredShipments":
                    order_delivery[
                        "delivered_shipments"
                    ],

                "remainingShipments":
                    order_delivery[
                        "remaining_shipments"
                    ]

            },

            "transportation":
            {

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

            "tracking":
            {

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

            "correlationId":
                correlation_id

        }

        # ====================================================
        # 22. PUBLISH OUTBOX EVENT
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
        # 23. LOG EVENT
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

                "total_shipments":
                    order_delivery[
                        "total_shipments"
                    ],

                "delivered_shipments":
                    order_delivery[
                        "delivered_shipments"
                    ],

                "remaining_shipments":
                    order_delivery[
                        "remaining_shipments"
                    ],

                "status":
                    "DELIVERED",

                "correlation_id":
                    correlation_id

            }
        )

        # ====================================================
        # 24. RETURN
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

            "total_shipments":
                order_delivery[
                    "total_shipments"
                ],

            "delivered_shipments":
                order_delivery[
                    "delivered_shipments"
                ],

            "remaining_shipments":
                order_delivery[
                    "remaining_shipments"
                ]
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

