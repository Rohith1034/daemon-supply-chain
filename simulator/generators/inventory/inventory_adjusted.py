from datetime import datetime, timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.ids import (
    next_adjustment_id
)

EVENT_NAME = "InventoryAdjusted"


def generate_inventory_adjusted():

    with Database() as db:

        # -------------------------------------------------------
        # Find completed cycle count requiring adjustment
        # -------------------------------------------------------

        task = db.fetch_one(
            """
            SELECT
                task_id,
                warehouse_id,
                product_id,
                location,
                assigned_worker_id,
                expected_quantity,
                received_quantity,
                discrepancy_quantity,
                correlation_id
            FROM warehouse_tasks
            WHERE
                task_type='CYCLE_COUNT'
                AND status='COMPLETED'
                AND discrepancy_quantity <> 0
                AND adjustment_processed = FALSE
            ORDER BY task_completed_at
            LIMIT 1
            """
        )

        if not task:
            raise Exception(
                "No completed cycle count requiring adjustment found"
            )

        # -------------------------------------------------------
        # Inventory
        # -------------------------------------------------------

        inventory = db.fetch_one(
            """
            SELECT
                inventory_id,
                on_hand_quantity
            FROM inventory
            WHERE
                product_id=%s
                AND warehouse_id=%s
                AND location_id=%s
            """,
            (
                task["product_id"],
                task["warehouse_id"],
                task["location"]
            )
        )

        if not inventory:
            raise Exception(
                "Inventory record not found"
            )

        previous_quantity = inventory["on_hand_quantity"]

        adjusted_quantity = task["received_quantity"]

        difference = (
            adjusted_quantity -
            previous_quantity
        )

        adjustment_id = next_adjustment_id(db)

        now = datetime.now(timezone.utc)

        # -------------------------------------------------------
        # Update inventory
        # -------------------------------------------------------

        db.execute(
            """
            UPDATE inventory
            SET
                on_hand_quantity=%s,
                last_updated_at=%s
            WHERE inventory_id=%s
            """,
            (
                adjusted_quantity,
                now,
                inventory["inventory_id"]
            )
        )

        # -------------------------------------------------------
        # Insert adjustment history
        # -------------------------------------------------------

        db.execute(
            """
            INSERT INTO inventory_adjustments
            (
                adjustment_id,
                warehouse_id,
                product_id,
                location_id,
                task_id,
                previous_quantity,
                adjusted_quantity,
                adjustment_difference,
                adjustment_reason,
                adjusted_by,
                correlation_id,
                created_at
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                adjustment_id,
                task["warehouse_id"],
                task["product_id"],
                task["location"],
                task["task_id"],
                previous_quantity,
                adjusted_quantity,
                difference,
                "CYCLE_COUNT_VARIANCE",
                task["assigned_worker_id"],
                task["correlation_id"],
                now
            )
        )

        # -------------------------------------------------------
        # Mark processed
        # -------------------------------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks
            SET adjustment_processed=TRUE
            WHERE task_id=%s
            """,
            (
                task["task_id"],
            )
        )

        # -------------------------------------------------------
        # Payload
        # -------------------------------------------------------

        payload = {

            "eventType": EVENT_NAME,

            "occurredAt": now.isoformat(),

            "inventoryAdjustment": {

                "adjustmentId": adjustment_id,

                "taskId": task["task_id"],

                "warehouseId": task["warehouse_id"],

                "locationId": task["location"],

                "productId": task["product_id"],

                "previousQuantity": previous_quantity,

                "adjustedQuantity": adjusted_quantity,

                "difference": difference,

                "reason": "CYCLE_COUNT_VARIANCE"

            },

            "correlationId": str(
                task["correlation_id"]
            )

        }

        # -------------------------------------------------------
        # Publish
        # -------------------------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="INVENTORY",

            aggregate_id=task["product_id"],

            correlation_id=str(
                task["correlation_id"]
            ),

            payload=payload

        )

        # -------------------------------------------------------
        # Success
        # -------------------------------------------------------

        log_event_success(

            EVENT_NAME,

            {

                "adjustment_id":
                    adjustment_id,

                "task_id":
                    task["task_id"],

                "warehouse_id":
                    task["warehouse_id"],

                "product_id":
                    task["product_id"],

                "previous_quantity":
                    previous_quantity,

                "adjusted_quantity":
                    adjusted_quantity,

                "difference":
                    difference,

                "correlation_id":
                    task["correlation_id"]

            }

        )


if __name__ == "__main__":

    try:

        generate_inventory_adjusted()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise