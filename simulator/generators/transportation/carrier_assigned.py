from datetime import timedelta, timezone
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "CarrierAssigned"


def _ensure_utc(value):
    """
    Normalize a datetime to timezone-aware UTC.

    PostgreSQL TIMESTAMPTZ values are generally returned as
    timezone-aware datetime objects, while the simulation
    clock may return a naive datetime.

    Normalizing both forms prevents datetime comparison
    errors.
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


def _get_assignment_time(shipment):
    """
    CarrierAssigned must occur after ShipmentReady.

    Primary business anchor:
        outbound_shipments.shipment_date

    Fallback:
        outbound_shipments.created_at
        simulation clock

    A small dispatch delay is added to represent the time
    required to assign transportation resources.
    """

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

    if shipment_date is not None:

        base_time = shipment_date

    elif created_at is not None:

        base_time = created_at

    else:

        base_time = simulation_now

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                15,
                120
            )
        )
    )


def generate_carrier_assigned(
    shipment_id=None
):

    with Database() as db:

        # =================================================
        # 1. FIND READY OUTBOUND SHIPMENT
        # =================================================
        #
        # outbound_shipments does NOT contain warehouse_id.
        #
        # warehouse_id belongs to outbound_fulfillment,
        # therefore we join fulfillment explicitly.
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
                    os.correlation_id

                FROM outbound_shipments os

                INNER JOIN outbound_fulfillment of
                    ON of.fulfillment_id =
                       os.fulfillment_id

                WHERE os.shipment_id=%s
                  AND os.shipment_status='READY'

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
                    os.correlation_id

                FROM outbound_shipments os

                INNER JOIN outbound_fulfillment of
                    ON of.fulfillment_id =
                       os.fulfillment_id

                WHERE os.shipment_status='READY'

                ORDER BY os.shipment_date

                LIMIT 1

                FOR UPDATE
                """
            )

        if not shipment:

            if shipment_id:

                raise Exception(
                    f"No READY outbound shipment found "
                    f"for shipment_id={shipment_id}"
                )

            raise Exception(
                "No READY outbound shipment found"
            )

        # =================================================
        # 2. EXTRACT SHIPMENT DETAILS
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

        correlation_id = str(
            shipment["correlation_id"]
        )

        # =================================================
        # 3. VALIDATE FULFILLMENT
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

        # =================================================
        # 4. CALCULATE ASSIGNMENT TIME
        # =================================================

        assigned_at = _get_assignment_time(
            shipment
        )

        # =================================================
        # 5. CHECK EXISTING TRANSPORTATION ASSIGNMENT
        # =================================================

        existing_assignment = db.fetch_one(
            """
            SELECT
                id,
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                assigned_at,
                status

            FROM outbound_shipment_transportation

            WHERE shipment_id=%s

            LIMIT 1

            FOR UPDATE
            """,
            (
                shipment_id,
            )
        )

        if existing_assignment:

            raise Exception(
                f"""
Transportation assignment already exists.

SHIPMENT:
{shipment_id}

ASSIGNMENT:
{existing_assignment["id"]}

STATUS:
{existing_assignment["status"]}
"""
            )

        # =================================================
        # 6. FIND AVAILABLE VEHICLE
        # =================================================

        vehicle = db.fetch_one(
            """
            SELECT
                vehicle_id

            FROM vehicles

            WHERE status='ACTIVE'

            ORDER BY random()

            LIMIT 1

            FOR UPDATE
            """
        )

        if not vehicle:

            raise Exception(
                "No ACTIVE vehicle found"
            )

        vehicle_id = vehicle[
            "vehicle_id"
        ]

        # =================================================
        # 7. FIND AVAILABLE TRAILER
        # =================================================

        trailer = db.fetch_one(
            """
            SELECT
                trailer_id

            FROM trailers

            WHERE status='ACTIVE'

            ORDER BY random()

            LIMIT 1

            FOR UPDATE
            """
        )

        if not trailer:

            raise Exception(
                "No ACTIVE trailer found"
            )

        trailer_id = trailer[
            "trailer_id"
        ]

        # =================================================
        # 8. FIND AVAILABLE DRIVER
        # =================================================

        driver = db.fetch_one(
            """
            SELECT
                driver_id

            FROM drivers

            WHERE status='ACTIVE'

            ORDER BY random()

            LIMIT 1

            FOR UPDATE
            """
        )

        if not driver:

            raise Exception(
                "No ACTIVE driver found"
            )

        driver_id = driver[
            "driver_id"
        ]

        # =================================================
        # 9. CREATE OUTBOUND TRANSPORTATION ASSIGNMENT
        # =================================================
        #
        # Resources are assigned here.
        #
        # They are NOT in transit yet.
        #
        # ASSIGNED happens at CarrierAssigned.
        # IN_TRANSIT happens after ShipmentPickedUp.
        # =================================================

        db.execute(
            """
            INSERT INTO outbound_shipment_transportation
            (
                shipment_id,
                vehicle_id,
                trailer_id,
                driver_id,
                assigned_at,
                status,
                created_at,
                updated_at
            )

            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                shipment_id,

                vehicle_id,

                trailer_id,

                driver_id,

                assigned_at,

                "ASSIGNED",

                assigned_at,

                assigned_at
            )
        )

        # =================================================
        # 10. UPDATE OUTBOUND SHIPMENT
        # =================================================

        db.execute(
            """
            UPDATE outbound_shipments

            SET
                shipment_status='ASSIGNED',

                updated_at=%s

            WHERE shipment_id=%s
            """,
            (
                assigned_at,

                shipment_id
            )
        )

        # =================================================
        # 11. UPDATE VEHICLE STATUS
        # =================================================

        db.execute(
            """
            UPDATE vehicles

            SET
                status='ASSIGNED'

            WHERE vehicle_id=%s
            """,
            (
                vehicle_id,
            )
        )

        # =================================================
        # 12. UPDATE TRAILER STATUS
        # =================================================

        db.execute(
            """
            UPDATE trailers

            SET
                status='ASSIGNED'

            WHERE trailer_id=%s
            """,
            (
                trailer_id,
            )
        )

        # =================================================
        # 13. UPDATE DRIVER STATUS
        # =================================================

        db.execute(
            """
            UPDATE drivers

            SET
                status='ASSIGNED'

            WHERE driver_id=%s
            """,
            (
                driver_id,
            )
        )

        # =================================================
        # 14. EVENT PAYLOAD
        # =================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                assigned_at.isoformat(),

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
                    "ASSIGNED",

                "assignedAt":
                    assigned_at.isoformat()

            },

            "transportation":
            {

                "status":
                    "ASSIGNED",

                "vehicleStatus":
                    "ASSIGNED",

                "trailerStatus":
                    "ASSIGNED",

                "driverStatus":
                    "ASSIGNED"

            },

            "correlationId":
                correlation_id
        }

        # =================================================
        # 15. PUBLISH OUTBOX EVENT
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
        # 16. LOG
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

                "vehicle_id":
                    vehicle_id,

                "trailer_id":
                    trailer_id,

                "driver_id":
                    driver_id,

                "assigned_at":
                    assigned_at,

                "status":
                    "ASSIGNED",

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

            "vehicle_id":
                vehicle_id,

            "trailer_id":
                trailer_id,

            "driver_id":
                driver_id,

            "status":
                "ASSIGNED",

            "assigned_at":
                assigned_at

        }


if __name__ == "__main__":

    try:

        import sys

        if len(sys.argv) > 1:

            generate_carrier_assigned(
                sys.argv[1]
            )

        else:

            generate_carrier_assigned()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
