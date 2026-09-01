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


EVENT_NAME = "ShipmentInTransit"


# ============================================================
# DATETIME NORMALIZATION
# ============================================================

def _ensure_utc(value):
    """
    Convert a datetime into timezone-aware UTC.

    PostgreSQL may return either naive or timezone-aware
    datetime objects depending on the column definition.
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
# IN-TRANSIT TIMESTAMP
# ============================================================

def _get_in_transit_time(
    picked_up_at
):
    """
    ShipmentInTransit must occur after ShipmentPickedUp.

    A short operational delay is added after pickup.
    """

    picked_up_at = _ensure_utc(
        picked_up_at
    )

    if picked_up_at is None:

        picked_up_at = _ensure_utc(
            get_simulation_now()
        )

    delay_minutes = random.randint(
        5,
        30
    )

    return (
        picked_up_at +
        timedelta(
            minutes=delay_minutes
        )
    )


# ============================================================
# MAIN GENERATOR
# ============================================================

def generate_shipment_in_transit(
    shipment_id=None
):

    with Database() as db:

        # ====================================================
        # 1. FIND PICKED-UP OUTBOUND SHIPMENT
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
                  AND os.shipment_status='PICKED_UP'
                  AND ost.status='PICKED_UP'

                LIMIT 1

                FOR UPDATE
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

                WHERE os.shipment_status='PICKED_UP'
                  AND ost.status='PICKED_UP'

                ORDER BY ost.picked_up_at ASC

                LIMIT 1

                FOR UPDATE
                """
            )

        if not shipment:

            if shipment_id:

                raise Exception(
                    f"No PICKED_UP outbound shipment found "
                    f"for shipment_id={shipment_id}"
                )

            raise Exception(
                "No PICKED_UP outbound shipment found"
            )

        # ====================================================
        # 2. EXTRACT DETAILS
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
        # 3. VALIDATE REQUIRED DATA
        # ====================================================

        if not fulfillment_id:

            raise Exception(
                f"Missing fulfillment_id for shipment "
                f"{shipment_id}"
            )

        if not warehouse_id:

            raise Exception(
                f"Missing warehouse_id for fulfillment "
                f"{fulfillment_id}"
            )

        if not order_id:

            raise Exception(
                f"Missing order_id for shipment "
                f"{shipment_id}"
            )

        if not package_id:

            raise Exception(
                f"Missing package_id for shipment "
                f"{shipment_id}"
            )

        if not transportation_id:

            raise Exception(
                f"No transportation record for shipment "
                f"{shipment_id}"
            )

        if not vehicle_id:

            raise Exception(
                f"No vehicle assigned to shipment "
                f"{shipment_id}"
            )

        if not trailer_id:

            raise Exception(
                f"No trailer assigned to shipment "
                f"{shipment_id}"
            )

        if not driver_id:

            raise Exception(
                f"No driver assigned to shipment "
                f"{shipment_id}"
            )

        # ====================================================
        # 4. VALIDATE TRANSPORTATION STATUS
        # ====================================================

        transportation_status = shipment[
            "transportation_status"
        ]

        if transportation_status != "PICKED_UP":

            raise Exception(
                f"""
Transportation is not PICKED_UP.

SHIPMENT:
{shipment_id}

STATUS:
{transportation_status}
"""
            )

        # ====================================================
        # 5. FIND EXISTING PICKED-UP TRACKING
        #
        # ShipmentPickedUp already created this row.
        #
        # ShipmentInTransit updates the same tracking row.
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
                departure_time,
                estimated_arrival,
                created_at,
                correlation_id

            FROM outbound_shipment_tracking

            WHERE shipment_id=%s
              AND status='PICKED_UP'

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
PICKED_UP tracking record not found.

SHIPMENT:
{shipment_id}

ShipmentPickedUp must create the
tracking row before ShipmentInTransit.
"""
            )

        tracking_id = tracking[
            "tracking_id"
        ]

        # ====================================================
        # 6. VALIDATE TRACKING RESOURCE MAPPING
        # ====================================================

        if (
            tracking["vehicle_id"] is not None
            and
            tracking["vehicle_id"] != vehicle_id
        ):

            raise Exception(
                f"""
Tracking vehicle mismatch.

SHIPMENT:
{shipment_id}

TRANSPORTATION VEHICLE:
{vehicle_id}

TRACKING VEHICLE:
{tracking["vehicle_id"]}
"""
            )

        if (
            tracking["trailer_id"] is not None
            and
            tracking["trailer_id"] != trailer_id
        ):

            raise Exception(
                f"""
Tracking trailer mismatch.

SHIPMENT:
{shipment_id}

TRANSPORTATION TRAILER:
{trailer_id}

TRACKING TRAILER:
{tracking["trailer_id"]}
"""
            )

        if (
            tracking["driver_id"] is not None
            and
            tracking["driver_id"] != driver_id
        ):

            raise Exception(
                f"""
Tracking driver mismatch.

SHIPMENT:
{shipment_id}

TRANSPORTATION DRIVER:
{driver_id}

TRACKING DRIVER:
{tracking["driver_id"]}
"""
            )

        # ====================================================
        # 7. DETERMINE PICKUP TIMESTAMP
        # ====================================================

        picked_up_at = _ensure_utc(
            shipment[
                "picked_up_at"
            ]
        )

        if picked_up_at is None:

            picked_up_at = _ensure_utc(
                tracking[
                    "departure_time"
                ]
            )

        if picked_up_at is None:

            raise Exception(
                f"""
No pickup timestamp available.

SHIPMENT:
{shipment_id}
"""
            )

        # ====================================================
        # 8. CALCULATE IN-TRANSIT TIMESTAMP
        # ====================================================

        in_transit_at = _get_in_transit_time(
            picked_up_at
        )

        if in_transit_at <= picked_up_at:

            raise Exception(
                f"""
Invalid shipment timeline.

SHIPMENT:
{shipment_id}

PICKED UP:
{picked_up_at}

IN TRANSIT:
{in_transit_at}
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
        # 10. GENERATE ETA
        # ====================================================

        transit_days = random.randint(
            1,
            4
        )

        estimated_arrival = (
            in_transit_at +
            timedelta(
                days=transit_days
            )
        )

        # ====================================================
        # 11. GENERATE GPS POSITION
        # ====================================================

        latitude = round(
            random.uniform(
                8.0,
                20.0
            ),
            6
        )

        longitude = round(
            random.uniform(
                72.0,
                85.0
            ),
            6
        )

        # ====================================================
        # 12. UPDATE OUTBOUND SHIPMENT
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipments

            SET
                shipment_status='IN_TRANSIT',
                updated_at=%s

            WHERE shipment_id=%s
              AND shipment_status='PICKED_UP'
            """,
            (
                in_transit_at,
                shipment_id,
            )
        )

        # ====================================================
        # 13. UPDATE TRANSPORTATION
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_transportation

            SET
                status='IN_TRANSIT',
                updated_at=%s

            WHERE id=%s
              AND shipment_id=%s
              AND status='PICKED_UP'
            """,
            (
                in_transit_at,
                transportation_id,
                shipment_id,
            )
        )

        # ====================================================
        # 14. UPDATE VEHICLE
        #
        # IMPORTANT:
        # The trailing comma is required because this is
        # a one-element parameter tuple.
        # ====================================================

        db.execute(
            """
            UPDATE vehicles

            SET
                status='IN_TRANSIT'

            WHERE vehicle_id=%s
            """,
            (
                vehicle_id,
            )
        )

        # ====================================================
        # 15. UPDATE TRAILER
        # ====================================================

        db.execute(
            """
            UPDATE trailers

            SET
                status='IN_TRANSIT'

            WHERE trailer_id=%s
            """,
            (
                trailer_id,
            )
        )

        # ====================================================
        # 16. UPDATE DRIVER
        # ====================================================

        db.execute(
            """
            UPDATE drivers

            SET
                status='IN_TRANSIT'

            WHERE driver_id=%s
            """,
            (
                driver_id,
            )
        )

        # ====================================================
        # 17. UPDATE EXISTING TRACKING
        #
        # DO NOT INSERT.
        #
        # The tracking_id generated by ShipmentPickedUp
        # remains unchanged throughout the shipment lifecycle.
        # ====================================================

        db.execute(
            """
            UPDATE outbound_shipment_tracking

            SET
                status='IN_TRANSIT',
                latitude=%s,
                longitude=%s,
                estimated_arrival=%s

            WHERE tracking_id=%s
              AND shipment_id=%s
              AND status='PICKED_UP'
            """,
            (
                latitude,
                longitude,
                estimated_arrival,
                tracking_id,
                shipment_id,
            )
        )

        # ====================================================
        # 18. VERIFY TRACKING UPDATE
        # ====================================================

        updated_tracking = db.fetch_one(
            """
            SELECT
                tracking_id,
                status
            FROM outbound_shipment_tracking

            WHERE tracking_id=%s
              AND shipment_id=%s

            LIMIT 1
            """,
            (
                tracking_id,
                shipment_id,
            )
        )

        if not updated_tracking:

            raise Exception(
                f"""
Tracking record disappeared after update.

TRACKING:
{tracking_id}

SHIPMENT:
{shipment_id}
"""
            )

        if updated_tracking["status"] != "IN_TRANSIT":

            raise Exception(
                f"""
Tracking status was not changed to IN_TRANSIT.

TRACKING:
{tracking_id}

STATUS:
{updated_tracking["status"]}
"""
            )

        # ====================================================
        # 19. EVENT PAYLOAD
        # ====================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                in_transit_at.isoformat(),

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

                "status":
                    "IN_TRANSIT",

                "pickedUpAt":
                    picked_up_at.isoformat(),

                "inTransitAt":
                    in_transit_at.isoformat(),

                "expectedDelivery":
                    expected_delivery.isoformat()
                    if expected_delivery
                    else None,

                "estimatedArrival":
                    estimated_arrival.isoformat(),

                "trackingId":
                    tracking_id,

                "latitude":
                    latitude,

                "longitude":
                    longitude
            },

            "transportation":
            {
                "status":
                    "IN_TRANSIT",

                "vehicleStatus":
                    "IN_TRANSIT",

                "trailerStatus":
                    "IN_TRANSIT",

                "driverStatus":
                    "IN_TRANSIT"
            },

            "tracking":
            {
                "trackingId":
                    tracking_id,

                "status":
                    "IN_TRANSIT",

                "latitude":
                    latitude,

                "longitude":
                    longitude,

                "departureTime":
                    picked_up_at.isoformat(),

                "estimatedArrival":
                    estimated_arrival.isoformat()
            },

            "correlationId":
                correlation_id
        }

        # ====================================================
        # 20. PUBLISH OUTBOX EVENT
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
        # 21. LOG
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

                "picked_up_at":
                    picked_up_at,

                "in_transit_at":
                    in_transit_at,

                "estimated_arrival":
                    estimated_arrival,

                "expected_delivery":
                    expected_delivery,

                "status":
                    "IN_TRANSIT",

                "correlation_id":
                    correlation_id
            }
        )

        # ====================================================
        # 22. RETURN
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
                "IN_TRANSIT",

            "picked_up_at":
                picked_up_at,

            "in_transit_at":
                in_transit_at,

            "estimated_arrival":
                estimated_arrival
        }


# ============================================================
# CLI ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        if len(sys.argv) > 1:

            generate_shipment_in_transit(
                sys.argv[1]
            )

        else:

            generate_shipment_in_transit()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
