from datetime import timedelta
import random

from core.db import Database
from core.ids import next_task_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "ReceivingTaskCreated"


def _get_receiving_task_time(shipment):
    """
    Receiving task creation happens after shipment delivery.

    Priority:
        1. actual_delivery
        2. shipment updated_at
        3. shipment_date
        4. simulation time
    """

    simulation_now = get_simulation_now()

    candidates = [
        shipment.get("actual_delivery"),
        shipment.get("updated_at"),
        shipment.get("shipment_date"),
        simulation_now
    ]

    candidates = [
        value
        for value in candidates
        if value is not None
    ]

    base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                10,
                120
            )
        )
    )


def _get_available_receiving_worker(db, warehouse_id):
    """
    Select worker capable of inbound receiving activity.
    """

    return db.fetch_one(
        """
        SELECT
            worker_id
        FROM workers
        WHERE warehouse_id=%s
          AND current_status='AVAILABLE'
          AND employment_status='Active'
          AND role IN
          (
            'Inventory Clerk',
            'Quality Inspector',
            'Forklift Operator',
            'Warehouse Associate',
            'Inbound Associate'
          )
        ORDER BY random()
        LIMIT 1
        """,
        (
            warehouse_id,
        )
    )


def _get_receiving_location(db, warehouse_id):
    """
    Fetch valid warehouse location.

    warehouse_tasks.location has FK dependency
    with warehouse_locations.location_id.
    """

    return db.fetch_one(
        """
        SELECT
            location_id
        FROM warehouse_locations
        WHERE warehouse_id=%s
          AND status='ACTIVE'
        ORDER BY random()
        LIMIT 1
        """,
        (
            warehouse_id,
        )
    )


def generate_receiving_task_created():

    with Database() as db:

        # ------------------------------------
        # Find delivered shipment
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id,
                total_quantity,
                shipment_date,
                actual_delivery,
                updated_at
            FROM shipments
            WHERE shipment_status='DELIVERED'
              AND receiving_task_created=false
            ORDER BY updated_at
            LIMIT 1
            """
        )


        if not shipment:
            raise Exception(
                "No delivered shipment available"
            )


        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )


        quantity = shipment["total_quantity"]


        # ------------------------------------
        # Business time
        # ------------------------------------

        task_time = _get_receiving_task_time(
            shipment
        )


        # ------------------------------------
        # Select receiving worker
        # ------------------------------------

        worker = _get_available_receiving_worker(
            db,
            warehouse_id
        )


        if not worker:
            raise Exception(
                f"No receiving worker available "
                f"for warehouse {warehouse_id}"
            )


        worker_id = worker["worker_id"]


        # ------------------------------------
        # Select receiving location
        # ------------------------------------

        location = _get_receiving_location(
            db,
            warehouse_id
        )


        if not location:
            raise Exception(
                f"No active warehouse location found "
                f"for warehouse {warehouse_id}"
            )


        location_id = location["location_id"]


        # ------------------------------------
        # Generate task id
        # ------------------------------------

        task_id = next_task_id(db)


        dock_location = "DOCK-001"


        # ------------------------------------
        # Create warehouse receiving task
        # ------------------------------------

        db.execute(
            """
            INSERT INTO warehouse_tasks
            (
                task_id,
                task_type,
                warehouse_id,
                shipment_id,
                quantity,
                priority,
                status,
                assigned_worker_id,
                estimated_minutes,
                created_at,
                assigned_at,
                created_by,
                correlation_id,
                dock_location,
                location,
                expected_quantity
            )
            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                task_id,
                "RECEIVING",
                warehouse_id,
                shipment_id,
                quantity,
                "HIGH",
                "CREATED",
                worker_id,
                30,

                # warehouse_tasks.created_at
                # is timestamp without timezone
                task_time.replace(
                    tzinfo=None
                ),

                task_time,

                "SYSTEM",

                correlation_id,

                dock_location,

                location_id,

                quantity
            )
        )


        # ------------------------------------
        # Prevent duplicate receiving tasks
        # ------------------------------------

        db.execute(
            """
            UPDATE shipments
            SET
                receiving_task_created=true,
                updated_at=%s
            WHERE shipment_id=%s
            """,
            (
                task_time,
                shipment_id
            )
        )


        # ------------------------------------
        # Event Payload
        # ------------------------------------

        payload = {

            "event_type":
                EVENT_NAME,


            "occurred_at":
                task_time.isoformat(),


            "receiving_task":
            {
                "task_id":
                    task_id,

                "shipment_id":
                    shipment_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "dock_location":
                    dock_location,

                "location_id":
                    location_id,

                "expected_quantity":
                    quantity,

                "status":
                    "CREATED"
            },


            "correlation_id":
                correlation_id
        }


        # ------------------------------------
        # Outbox
        # ------------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="WAREHOUSE_TASK",
            aggregate_id=task_id,
            correlation_id=correlation_id,
            payload=payload
        )


        # ------------------------------------
        # Log
        # ------------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "task_id":
                    task_id,

                "shipment_id":
                    shipment_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "location_id":
                    location_id,

                "quantity":
                    quantity,

                "correlation_id":
                    correlation_id
            }
        )


        print(
f"""
============================================================
EVENT : {EVENT_NAME}

TASK ID             : {task_id}
SHIPMENT ID         : {shipment_id}
WAREHOUSE ID        : {warehouse_id}
WORKER ID           : {worker_id}
LOCATION ID         : {location_id}
QUANTITY            : {quantity}

CORRELATION ID      : {correlation_id}

TIME : {task_time}

STATUS : SUCCESS
============================================================
"""
        )



if __name__ == "__main__":

    try:

        generate_receiving_task_created()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise