from datetime import timedelta
import random
import uuid


from core.db import Database

from core.outbox import publish_event

from core.payloads import (
    build_inventory_putaway_payload
)

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "InventoryPutaway"


def _get_putaway_time(inventories):
    """
    Calculate a causally valid putaway timestamp.

    InventoryPutaway must happen after the inventory was
    received and its stock quantity was updated.

    The latest last_updated_at value among the selected
    inventory records is used as the business anchor.

    The simulation clock is also considered so that the
    event remains compatible with the overall simulation
    timeline.
    """

    simulation_now = get_simulation_now()

    inventory_times = [
        inv["last_updated_at"]
        for inv in inventories
        if inv.get("last_updated_at") is not None
    ]

    candidates = inventory_times + [simulation_now]

    base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                10,
                60
            )
        )
    )


def generate_inventory_putaway():

    with Database() as db:

        # ---------------------------------
        # Find inventory waiting for putaway
        #
        # Inventory must:
        #
        # 1. Have RECEIVED status
        # 2. Not yet have a warehouse location
        #
        # AVAILABLE is intentionally NOT selected
        # here because already-putaway inventory
        # must never be processed again.
        # ---------------------------------

        inventories = db.fetch_all(
            """
            SELECT *
            FROM inventory
            WHERE inventory_status='RECEIVED'
              AND location_id IS NULL
            ORDER BY last_updated_at
            LIMIT 100
            """
        )

        if not inventories:

            raise Exception(
                "No RECEIVED inventory waiting for putaway"
            )

        # ---------------------------------
        # Calculate putaway business time
        # ---------------------------------

        putaway_at = _get_putaway_time(
            inventories
        )

        putaway_items = []

        correlation_id = str(
            uuid.uuid4()
        )

        # ---------------------------------
        # Process inventory records
        # ---------------------------------

        for inv in inventories:

            # -----------------------------
            # Find bin location
            # -----------------------------

            location = db.fetch_one(
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
                    inv["warehouse_id"],
                )
            )

            if not location:

                raise Exception(
                    "No warehouse location found "
                    f"for warehouse "
                    f"{inv['warehouse_id']}"
                )

            location_id = location[
                "location_id"
            ]

            # -----------------------------
            # Insert location stock
            # -----------------------------

            db.execute(
                """
                INSERT INTO inventory_locations
                (
                    product_id,
                    warehouse_id,
                    location_id,
                    quantity
                )
                VALUES
                (%s,%s,%s,%s)
                ON CONFLICT
                (
                    product_id,
                    warehouse_id,
                    location_id
                )
                DO UPDATE SET
                    quantity =
                        inventory_locations.quantity
                        + EXCLUDED.quantity
                """,
                (
                    inv["product_id"],
                    inv["warehouse_id"],
                    location_id,
                    inv["on_hand_quantity"]
                )
            )

            # -----------------------------
            # Update inventory
            #
            # THIS is where inventory becomes
            # AVAILABLE.
            # -----------------------------

            db.execute(
                """
                UPDATE inventory
                SET
                    location_id=%s,
                    inventory_status='AVAILABLE',
                    last_updated_at=%s
                WHERE inventory_id=%s
                """,
                (
                    location_id,
                    putaway_at,
                    inv["inventory_id"]
                )
            )

            # -----------------------------
            # Prepare event item
            # -----------------------------

            putaway_items.append(
                {
                    "product_id":
                        inv["product_id"],

                    "warehouse_id":
                        inv["warehouse_id"],

                    "quantity":
                        inv["on_hand_quantity"],

                    "location_id":
                        location_id
                }
            )

        # ---------------------------------
        # Event payload
        # ---------------------------------

        payload = build_inventory_putaway_payload(
            warehouse_id=
                inventories[0]["warehouse_id"],

            items=
                putaway_items,

            correlation_id=
                correlation_id
        )

        # ---------------------------------
        # Publish Outbox Event
        # ---------------------------------

        publish_event(
            db=db,

            event_type=EVENT_NAME,

            aggregate_type="INVENTORY",

            aggregate_id=
                inventories[0]["warehouse_id"],

            correlation_id=
                correlation_id,

            payload=payload
        )

        # ---------------------------------
        # Logging
        # ---------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "warehouse_id":
                    inventories[0]["warehouse_id"],

                "items":
                    len(putaway_items),

                "putaway_at":
                    putaway_at,

                "inventory_status":
                    "AVAILABLE",

                "correlation_id":
                    correlation_id
            }
        )


if __name__ == "__main__":

    try:

        generate_inventory_putaway()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
