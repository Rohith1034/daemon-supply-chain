from datetime import timedelta
import random
import uuid


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "InventoryReserved"


def _get_reserved_time(order, allocations):
    """
    InventoryReserved must occur after the inventory allocation
    has already been created.

    The allocation timestamps are therefore the primary business
    anchor. The order confirmation/date and simulation clock are
    used as additional safeguards.

    This function only calculates the event timestamp. It does
    not modify inventory quantities.
    """

    simulation_now = get_simulation_now()

    allocation_times = [
        allocation["allocated_at"]
        for allocation in allocations
        if allocation.get("allocated_at") is not None
    ]

    candidates = [
        candidate
        for candidate in [
            *allocation_times,
            order.get("confirmed_at"),
            order.get("order_date"),
            order.get("created_at"),
            simulation_now
        ]
        if candidate is not None
    ]

    if not candidates:
        base_time = simulation_now
    else:
        base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                1,
                15
            )
        )
    )


def generate_inventory_allocation_created(order_id):

    with Database() as db:

        # =====================================================
        # 1. GET ORDER
        # =====================================================

        order = db.fetch_one(
            """
            SELECT
                order_id,
                warehouse_id,
                correlation_id,
                order_status,
                order_date,
                created_at,
                confirmed_at
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
                f"""
Order not found.

ORDER:
{order_id}
"""
            )

        # -----------------------------------------------------
        # Allocation creation should already have moved the
        # order to ALLOCATED.
        # -----------------------------------------------------

        if order["order_status"] != "ALLOCATED":

            raise Exception(
                f"""
Order is not in ALLOCATED state.

ORDER:
{order_id}

STATUS:
{order["order_status"]}
"""
            )

        warehouse_id = order["warehouse_id"]

        correlation_id = (
            str(order["correlation_id"])
            if order["correlation_id"]
            else str(uuid.uuid4())
        )

        # =====================================================
        # 2. GET ORDER ITEMS
        # =====================================================

        items = db.fetch_all(
            """
            SELECT
                order_item_id,
                product_id,
                quantity
            FROM order_items
            WHERE order_id=%s
            ORDER BY order_item_id
            """,
            (
                order_id,
            )
        )

        if not items:

            raise Exception(
                f"""
No order items found.

ORDER:
{order_id}
"""
            )

        # =====================================================
        # 3. GET EXISTING RESERVATIONS
        #
        # InventoryAllocationCreated already created the
        # allocation records and increased reserved_quantity.
        #
        # InventoryReserved only confirms that state.
        # =====================================================

        allocations = db.fetch_all(
            """
            SELECT
                allocation_id,
                product_id,
                allocated_quantity,
                allocation_status,
                allocated_at,
                inventory_id,
                location_id
            FROM inventory_allocations
            WHERE order_id=%s
              AND allocation_status='RESERVED'
            ORDER BY allocation_id
            FOR UPDATE
            """,
            (
                order_id,
            )
        )

        if not allocations:

            raise Exception(
                f"""
No RESERVED inventory allocations found.

ORDER:
{order_id}
"""
            )

        # =====================================================
        # 4. VALIDATE COMPLETE ORDER COVERAGE
        # =====================================================

        allocation_by_product = {}

        for allocation in allocations:

            product_id = allocation["product_id"]

            allocation_by_product[product_id] = (
                allocation_by_product.get(
                    product_id,
                    0
                )
                +
                allocation["allocated_quantity"]
            )

        for item in items:

            product_id = item["product_id"]

            required_quantity = item["quantity"]

            reserved_quantity = allocation_by_product.get(
                product_id,
                0
            )

            if reserved_quantity < required_quantity:

                raise Exception(
                    f"""
Incomplete inventory reservation.

ORDER:
{order_id}

PRODUCT:
{product_id}

REQUIRED:
{required_quantity}

RESERVED:
{reserved_quantity}
"""
                )

        # =====================================================
        # 5. VALIDATE INVENTORY STATE
        #
        # Reservation must only exist against inventory that
        # is already AVAILABLE after putaway.
        # =====================================================

        for allocation in allocations:

            inventory = db.fetch_one(
                """
                SELECT
                    inventory_id,
                    product_id,
                    warehouse_id,
                    location_id,
                    inventory_status,
                    on_hand_quantity,
                    reserved_quantity
                FROM inventory
                WHERE inventory_id=%s
                FOR UPDATE
                """,
                (
                    allocation["inventory_id"],
                )
            )

            if not inventory:

                raise Exception(
                    f"""
Inventory not found for allocation.

ALLOCATION:
{allocation["allocation_id"]}

INVENTORY:
{allocation["inventory_id"]}
"""
                )

            if inventory["inventory_status"] != "AVAILABLE":

                raise Exception(
                    f"""
Inventory is not AVAILABLE for reservation.

INVENTORY:
{inventory["inventory_id"]}

PRODUCT:
{inventory["product_id"]}

STATUS:
{inventory["inventory_status"]}
"""
                )

            if not inventory["location_id"]:

                raise Exception(
                    f"""
Inventory location missing for reserved inventory.

INVENTORY:
{inventory["inventory_id"]}

PRODUCT:
{inventory["product_id"]}
"""
                )

            if (
                inventory["reserved_quantity"]
                >
                inventory["on_hand_quantity"]
            ):

                raise Exception(
                    f"""
Invalid reservation quantity.

INVENTORY:
{inventory["inventory_id"]}

ON HAND:
{inventory["on_hand_quantity"]}

RESERVED:
{inventory["reserved_quantity"]}
"""
                )

        # =====================================================
        # 6. CALCULATE RESERVATION EVENT TIME
        # =====================================================

        reserved_at = _get_reserved_time(
            order,
            allocations
        )

        # =====================================================
        # 7. PREPARE EVENT ALLOCATIONS
        # =====================================================

        allocation_payload = []

        for allocation in allocations:

            allocation_payload.append(
                {
                    "allocationId":
                        allocation["allocation_id"],

                    "productId":
                        allocation["product_id"],

                    "inventoryId":
                        allocation["inventory_id"],

                    "locationId":
                        allocation["location_id"],

                    "quantity":
                        allocation["allocated_quantity"],

                    "status":
                        "RESERVED",

                    "reservedAt":
                        reserved_at.isoformat()
                }
            )

        # =====================================================
        # 8. UPDATE ORDER FLAG
        #
        # Preserve your existing items_created behavior.
        # This does not change the order's ALLOCATED status.
        # =====================================================

        db.execute(
            """
            UPDATE orders
            SET
                items_created=true
            WHERE order_id=%s
            """,
            (
                order_id,
            )
        )

        # =====================================================
        # 9. EVENT PAYLOAD
        # =====================================================

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                reserved_at.isoformat(),

            "order":
            {
                "orderId":
                    order_id,

                "warehouseId":
                    warehouse_id,

                "status":
                    "ALLOCATED"
            },

            "allocations":
                allocation_payload,

            "reservationStatus":
                "RESERVED",

            "correlationId":
                correlation_id
        }

        # =====================================================
        # 10. OUTBOX EVENT
        # =====================================================

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="ORDER",
            aggregate_id=order_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # =====================================================
        # 11. LOG
        # =====================================================

        log_event_success(
            EVENT_NAME,
            {
                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "allocation_count":
                    len(allocations),

                "reserved_at":
                    reserved_at,

                "inventory_status":
                    "AVAILABLE",

                "allocation_status":
                    "RESERVED",

                "correlation_id":
                    correlation_id
            }
        )

        return {
            "order_id":
                order_id,

            "allocations":
                allocation_payload,

            "status":
                "RESERVED"
        }


if __name__ == "__main__":

    import sys

    try:

        if len(sys.argv) < 2:

            raise Exception(
                """
Missing order id.

Usage:

python inventory_reserved.py ORD-000000001
"""
            )

        generate_inventory_allocation_created(
            sys.argv[1]
        )

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
