from datetime import timedelta, timezone
import random

from core.db import Database

from core.ids import (
    next_loading_id,
    next_tracking_id
)

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)

EVENT_NAME = "ShipmentPickedUp"


def _ensure_utc(value):
    """
    Normalize a datetime to timezone-aware UTC.

    Database timestamps may already be timezone-aware while
    the simulation clock can potentially provide a naive value.
    All timestamps are normalized before being used together.
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


def _get_pickup_time(
        shipment,
        transportation
):
    """
    ShipmentPickedUp must occur after CarrierAssigned.

    Primary business anchor:
        outbound_shipment_transportation.assigned_at

    Fallback anchors:
        outbound_shipments.shipment_date
        outbound_shipments.created_at
        simulation clock

    A physical loading and dispatch delay is added before
    the shipment is considered picked up.
    """

    assigned_at = _ensure_utc(
        transportation.get(
            "assigned_at"
        )
    )

    shipment_date = _ensure_utc(
        shipment.get(
            "shipment_date"
        )
    )

    created_at = _ensure_utc(
        shipment.get(
            "created_at"
        )
    )

    simulation_now = _ensure_utc(
        get_simulation_now()
    )

    # ---------------------------------------------
    # The transportation assignment is the
    # authoritative predecessor.
    # ---------------------------------------------

    if assigned_at is not None:

        base_time = assigned_at

    elif shipment_date is not None:

        base_time = shipment_date

    elif created_at is not None:

        base_time = created_at

    else:

        base_time = simulation_now

    return (
            base_time +
            timedelta(
                minutes=random.randint(
                    30,
                    180
                )
            )
    )


def generate_shipment_picked_up(
        shipment_id=None
):
    with Database() as db:

        # =================================================
        # 1. FIND ASSIGNED OUTBOUND SHIPMENT
        # =================================================
        #
        # warehouse_id belongs to outbound_fulfillment,
        # not outbound_shipments.
        #
        # The transportation assignment is joined because
        # CarrierAssigned must already exist.
        # =================================================

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
                    os.created_at,
                    os.updated_at,
                    os.correlation_id,

                    ost.id AS transportation_id,
                    ost.assigned_at,
                    ost.vehicle_id,
                    ost.trailer_id,
                    ost.driver_id,
                    ost.status AS transportation_status

                FROM outbound_shipments os

                INNER JOIN outbound_fulfillment of
                    ON of.fulfillment_id =
                       os.fulfillment_id

                INNER JOIN outbound_shipment_transportation ost
                    ON ost.shipment_id =
                       os.shipment_id

                WHERE os.shipment_id=%s
                  AND os.shipment_status='ASSIGNED'
                  AND ost.status='ASSIGNED'

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
                    os.created_at,
                    os.updated_at,
                    os.correlation_id,

                    ost.id AS transportation_id,
                    ost.assigned_at,
                    ost.vehicle_id,
                    ost.trailer_id,
                    ost.driver_id,
                    ost.status AS transportation_status

                FROM outbound_shipments os

                INNER JOIN outbound_fulfillment of
                    ON of.fulfillment_id =
                       os.fulfillment_id

                INNER JOIN outbound_shipment_transportation ost
                    ON ost.shipment_id =
                       os.shipment_id

                WHERE os.shipment_status='ASSIGNED'
                  AND ost.status='ASSIGNED'

                ORDER BY ost.assigned_at

                LIMIT 1

                FOR UPDATE
                """
            )

        if not shipment:

            if shipment_id:
                raise Exception(
                    f"No ASSIGNED outbound shipment found "
                    f"for shipment_id={shipment_id}"
                )

            raise Exception(
                "No ASSIGNED outbound shipment found"
            )

        # =================================================
        # 2. EXTRACT DETAILS
        # =================================================

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

        correlation_id = str(
            shipment["correlation_id"]
        )

        # =================================================
        # 3. VALIDATE REQUIRED RELATIONSHIPS
        # =================================================

        if not fulfillment_id:
            raise Exception(
                f"""
Outbound shipment has no fulfillment_id.

SHIPMENT:
{shipment_id}
"""
            )

        if not warehouse_id:
            raise Exception(
                f"""
Outbound fulfillment has no warehouse_id.

FULFILLMENT:
{fulfillment_id}

SHIPMENT:
{shipment_id}
"""
            )

        if not order_id:
            raise Exception(
                f"""
Outbound shipment has no order_id.

SHIPMENT:
{shipment_id}
"""
            )

        if not package_id:
            raise Exception(
                f"""
Outbound shipment has no package_id.

SHIPMENT:
{shipment_id}
"""
            )

        if not vehicle_id:
            raise Exception(
                f"""
Outbound shipment has no assigned vehicle.

SHIPMENT:
{shipment_id}
"""
            )

        if not trailer_id:
            raise Exception(
                f"""
Outbound shipment has no assigned trailer.

SHIPMENT:
{shipment_id}
"""
            )

        if not driver_id:
            raise Exception(
                f"""
Outbound shipment has no assigned driver.

SHIPMENT:
{shipment_id}
"""
            )

        # =================================================
        # 4. BUILD TRANSPORTATION CONTEXT
        # =================================================

        transportation = {
            "assigned_at":
                shipment["assigned_at"]
        }

        # =================================================
        # 5. CALCULATE PICKUP TIME
        # =================================================

        picked_up_at = _get_pickup_time(
            shipment,
            transportation
        )

        # =================================================
        # 6. VALIDATE TRANSPORTATION STATUS
        # =================================================

        if shipment[
            "transportation_status"
        ] != "ASSIGNED":
            raise Exception(
                f"""
Transportation assignment is not ASSIGNED.

SHIPMENT:
{shipment_id}

STATUS:
{shipment["transportation_status"]}
"""
            )

        # =================================================
        # 7. CHECK EXISTING LOADING EVENT
        # =================================================

        existing_loading = db.fetch_one(
            """
            SELECT
                loading_id
            FROM outbound_shipment_loading_events
            WHERE shipment_id=%s
            LIMIT 1
            """,
            (
                shipment_id,
            )
        )

        if existing_loading:
            raise Exception(
                f"""
Outbound loading event already exists.

SHIPMENT:
{shipment_id}

LOADING:
{existing_loading["loading_id"]}
"""
            )

        # =================================================
        # 8. GET PACKAGE QUANTITY
        # =================================================

        package = db.fetch_one(
            """
            SELECT
                package_id,
                order_id,
                warehouse_id,
                total_quantity,
                package_status
            FROM packages
            WHERE package_id=%s
            LIMIT 1
            """,
            (
                package_id,
            )
        )

        if not package:
            raise Exception(
                f"""
Package not found.

PACKAGE:
{package_id}
"""
            )

        if package["order_id"] != order_id:
            raise Exception(
                f"""
Package/order mismatch.

PACKAGE:
{package_id}

PACKAGE ORDER:
{package["order_id"]}

SHIPMENT ORDER:
{order_id}
"""
            )

        if package["warehouse_id"] != warehouse_id:
            raise Exception(
                f"""
Package/warehouse mismatch.

PACKAGE:
{package_id}

PACKAGE WAREHOUSE:
{package["warehouse_id"]}

FULFILLMENT WAREHOUSE:
{warehouse_id}
"""
            )

        allowed_package_states = (
            "PACKED",
            "READY_FOR_SHIPMENT"
        )

        if package["package_status"] not in allowed_package_states:
            raise Exception(
                f"""
        Package is not ready for pickup.

        PACKAGE:
        {package_id}

        CURRENT STATUS:
        {package["package_status"]}

        ALLOWED:
        {allowed_package_states}
        """
            )

        loaded_quantity = package[
            "total_quantity"
        ]

        if loaded_quantity is None or loaded_quantity <= 0:
            raise Exception(
                f"""
                    Invalid package quantity.
                    
                    PACKAGE:
                    {package_id}
                    
                    QUANTITY:
                    {loaded_quantity}
                    """
                                )

        # =================================================
        # 9. GENERATE LOADING ID
        # =================================================

        loading_id = next_loading_id(
            db
        )

        # =================================================
        # 10. CREATE LOADING EVENT
        # =================================================

        db.execute(
            """
            INSERT INTO outbound_shipment_loading_events
            (
                loading_id,
                shipment_id,
                warehouse_id,
                vehicle_id,
                trailer_id,
                loaded_quantity,
                loading_status,
                loaded_at,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                loading_id,
                shipment_id,
                warehouse_id,
                vehicle_id,
                trailer_id,
                loaded_quantity,
                "COMPLETED",
                picked_up_at,
                correlation_id
            )
        )

        # =================================================
        # 11. UPDATE OUTBOUND SHIPMENT
        # =================================================

        db.execute(
            """
            UPDATE outbound_shipments
            SET
                shipment_status='PICKED_UP',
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                picked_up_at,
                shipment_id
            )
        )

        # =================================================
        # 12. UPDATE TRANSPORTATION
        # =================================================

        db.execute(
            """
            UPDATE outbound_shipment_transportation
            SET
                status='PICKED_UP',
                picked_up_at=%s,
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                picked_up_at,
                picked_up_at,
                shipment_id
            )
        )

        # =================================================
        # 13. VEHICLE -> IN_TRANSIT
        # =================================================

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

        # =================================================
        # 14. TRAILER -> IN_TRANSIT
        # =================================================

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

        # =================================================
        # 15. DRIVER -> IN_TRANSIT
        # =================================================

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

        # =================================================
        # 16. CREATE PICKED_UP TRACKING
        # =================================================

        tracking_id = next_tracking_id(
            db
        )

        db.execute(
            """
            INSERT INTO outbound_shipment_tracking
            (
                tracking_id,
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                status,
                departure_time,
                created_at,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                tracking_id,
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                "PICKED_UP",
                picked_up_at,
                picked_up_at,
                correlation_id
            )
        )

        # =================================================
        # 17. EVENT PAYLOAD
        # =================================================

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                picked_up_at.isoformat(),

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
                        "PICKED_UP",

                    "pickedUpAt":
                        picked_up_at.isoformat()
                },

            "loading":
                {
                    "loadingId":
                        loading_id,

                    "loadedQuantity":
                        loaded_quantity,

                    "status":
                        "COMPLETED",

                    "loadedAt":
                        picked_up_at.isoformat()
                },

            "tracking":
                {
                    "trackingId":
                        tracking_id,

                    "status":
                        "PICKED_UP",

                    "departureTime":
                        picked_up_at.isoformat()
                },

            "transportation":
                {
                    "status":
                        "PICKED_UP",

                    "vehicleStatus":
                        "IN_TRANSIT",

                    "trailerStatus":
                        "IN_TRANSIT",

                    "driverStatus":
                        "IN_TRANSIT"
                },

            "correlationId":
                correlation_id
        }

        # =================================================
        # 18. PUBLISH OUTBOX EVENT
        # =================================================

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="OUTBOUND_SHIPMENT",
            aggregate_id=shipment_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # =================================================
        # 19. LOG
        # =================================================

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

                "loading_id":
                    loading_id,

                "tracking_id":
                    tracking_id,

                "vehicle_id":
                    vehicle_id,

                "trailer_id":
                    trailer_id,

                "driver_id":
                    driver_id,

                "picked_up_at":
                    picked_up_at,

                "status":
                    "PICKED_UP",

                "correlation_id":
                    correlation_id
            }
        )

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

            "loading_id":
                loading_id,

            "tracking_id":
                tracking_id,

            "vehicle_id":
                vehicle_id,

            "trailer_id":
                trailer_id,

            "driver_id":
                driver_id,

            "status":
                "PICKED_UP",

            "picked_up_at":
                picked_up_at
        }


if __name__ == "__main__":

    try:

        import sys

        if len(sys.argv) > 1:

            generate_shipment_picked_up(
                sys.argv[1]
            )

        else:

            generate_shipment_picked_up()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
