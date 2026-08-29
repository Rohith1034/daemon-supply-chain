from datetime import datetime, timezone
import sys

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "InventoryReserved"


def generate_inventory_reserved(order_id):

    with Database() as db:

        # =====================================================
        # 1. GET EXACT ORDER
        # =====================================================

        order = db.fetch_one(
            """
            SELECT
                order_id,
                warehouse_id,
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
                f"""
Order not found.

ORDER:
{order_id}
"""
            )

        if order["order_status"] != "CREATED":

            raise Exception(
                f"""
Order is not in CREATED state.

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
            else None
        )

        if not correlation_id:

            raise Exception(
                f"""
Order has no correlation_id.

ORDER:
{order_id}
"""
            )

        # =====================================================
        # 2. GET ALL ALLOCATED ITEMS FOR THIS ORDER
        # =====================================================

        allocations = db.fetch_all(
            """
            SELECT
                allocation_id,
                order_id,
                warehouse_id,
                product_id,
                allocated_quantity,
                allocation_status,
                inventory_id,
                location_id,
                correlation_id
            FROM inventory_allocations
            WHERE order_id=%s
              AND allocation_status='ALLOCATED'
            ORDER BY allocated_at, allocation_id
            FOR UPDATE
            """,
            (
                order_id,
            )
        )

        if not allocations:

            raise Exception(
                f"""
No ALLOCATED inventory found for order.

ORDER:
{order_id}

Expected state:
InventoryAllocationCreated -> ALLOCATED
InventoryReserved -> RESERVED
"""
            )

        now = datetime.now(
            timezone.utc
        )

        reservations = []

        # =====================================================
        # 3. PROCESS EVERY ALLOCATION
        # =====================================================

        for allocation in allocations:

            allocation_id = allocation["allocation_id"]

            product_id = allocation["product_id"]

            allocated_quantity = allocation["allocated_quantity"]

            inventory_id = allocation["inventory_id"]

            location_id = allocation["location_id"]

            allocation_warehouse = allocation["warehouse_id"]

            # -------------------------------------------------
            # Basic validation
            # -------------------------------------------------

            if not inventory_id:

                raise Exception(
                    f"""
Allocation does not contain inventory_id.

ALLOCATION:
{allocation_id}
"""
                )

            if not location_id:

                raise Exception(
                    f"""
Allocation does not contain location_id.

ALLOCATION:
{allocation_id}
"""
                )

            if not allocated_quantity or allocated_quantity <= 0:

                raise Exception(
                    f"""
Invalid allocated quantity.

ALLOCATION:
{allocation_id}

QUANTITY:
{allocated_quantity}
"""
                )

            if allocation_warehouse != warehouse_id:

                raise Exception(
                    f"""
Allocation warehouse mismatch.

ORDER WAREHOUSE:
{warehouse_id}

ALLOCATION WAREHOUSE:
{allocation_warehouse}

ALLOCATION:
{allocation_id}
"""
                )

            # =================================================
            # 4. LOCK EXACT INVENTORY ROW
            # =================================================

            inventory = db.fetch_one(
                """
                SELECT
                    inventory_id,
                    product_id,
                    warehouse_id,
                    location_id,
                    on_hand_quantity,
                    reserved_quantity,
                    damaged_quantity,
                    available_quantity
                FROM inventory
                WHERE inventory_id=%s
                FOR UPDATE
                """,
                (
                    inventory_id,
                )
            )

            if not inventory:

                raise Exception(
                    f"""
Inventory record not found.

INVENTORY:
{inventory_id}

ALLOCATION:
{allocation_id}
"""
                )

            # -------------------------------------------------
            # Validate inventory relationship
            # -------------------------------------------------

            if inventory["product_id"] != product_id:

                raise Exception(
                    f"""
Inventory/product mismatch.

ALLOCATION:
{allocation_id}

EXPECTED PRODUCT:
{product_id}

INVENTORY PRODUCT:
{inventory["product_id"]}
"""
                )

            if inventory["warehouse_id"] != warehouse_id:

                raise Exception(
                    f"""
Inventory/warehouse mismatch.

ALLOCATION:
{allocation_id}

EXPECTED WAREHOUSE:
{warehouse_id}

INVENTORY WAREHOUSE:
{inventory["warehouse_id"]}
"""
                )

            if inventory["location_id"] != location_id:

                raise Exception(
                    f"""
Inventory/location mismatch.

ALLOCATION:
{allocation_id}

EXPECTED LOCATION:
{location_id}

INVENTORY LOCATION:
{inventory["location_id"]}
"""
                )

            # =================================================
            # 5. CALCULATE AVAILABLE INVENTORY
            # =================================================

            available_quantity = (
                inventory["on_hand_quantity"]
                -
                inventory["reserved_quantity"]
                -
                inventory["damaged_quantity"]
            )

            if available_quantity < allocated_quantity:

                raise Exception(
                    f"""
Insufficient inventory for reservation.

PRODUCT:
{product_id}

INVENTORY:
{inventory_id}

AVAILABLE:
{available_quantity}

REQUIRED:
{allocated_quantity}
"""
                )

            # =================================================
            # 6. INCREASE RESERVED QUANTITY
            # =================================================

            db.execute(
                """
                UPDATE inventory
                SET
                    reserved_quantity =
                        reserved_quantity + %s,
                    inventory_status='RESERVED',
                    last_updated_at=%s
                WHERE inventory_id=%s
                """,
                (
                    allocated_quantity,
                    now,
                    inventory_id
                )
            )

            # =================================================
            # 7. CHANGE ALLOCATION STATE
            # =================================================

            updated = db.fetch_one(
                """
                UPDATE inventory_allocations
                SET
                    allocation_status='RESERVED'
                WHERE allocation_id=%s
                  AND allocation_status='ALLOCATED'
                RETURNING allocation_id
                """,
                (
                    allocation_id,
                )
            )

            if not updated:

                raise Exception(
                    f"""
Failed to transition allocation to RESERVED.

ALLOCATION:
{allocation_id}
"""
                )

            # =================================================
            # 8. CREATE RESERVATION RECORD
            # =================================================

            existing_reservation = db.fetch_one(
                """
                SELECT
                    reservation_id,
                    quantity
                FROM inventory_reservations
                WHERE order_id=%s
                  AND product_id=%s
                  AND reservation_status='RESERVED'
                LIMIT 1
                """,
                (
                    order_id,
                    product_id
                )
            )

            if existing_reservation:

                reservation_id = (
                    existing_reservation["reservation_id"]
                )

            else:

                reservation_id = (
                    f"RES-{allocation_id.replace('ALLOC-', '')}"
                )

                db.execute(
                    """
                    INSERT INTO inventory_reservations
                    (
                        reservation_id,
                        order_id,
                        product_id,
                        warehouse_id,
                        quantity,
                        reservation_status,
                        reserved_at,
                        correlation_id
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s,%s,%s,%s
                    )
                    """,
                    (
                        reservation_id,
                        order_id,
                        product_id,
                        warehouse_id,
                        allocated_quantity,
                        "RESERVED",
                        now,
                        correlation_id
                    )
                )

            # =================================================
            # 9. INVENTORY TRANSACTION
            # =================================================

            db.execute(
                """
                INSERT INTO inventory_transactions
                (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    transaction_type,
                    quantity,
                    reference_type,
                    reference_id,
                    correlation_id
                )
                VALUES
                (
                    %s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    "RESERVATION",
                    allocated_quantity,
                    "ORDER",
                    order_id,
                    correlation_id
                )
            )

            reservations.append(
                {
                    "reservationId": reservation_id,
                    "allocationId": allocation_id,
                    "productId": product_id,
                    "inventoryId": inventory_id,
                    "locationId": location_id,
                    "quantity": allocated_quantity
                }
            )

        # =====================================================
        # 10. UPDATE ORDER STATUS
        # =====================================================

        db.execute(
            """
            UPDATE orders
            SET
                order_status='RESERVED'
            WHERE order_id=%s
              AND order_status='CREATED'
            """,
            (
                order_id,
            )
        )

        # =====================================================
        # 11. EVENT PAYLOAD
        # =====================================================

        payload = {
            "eventType": EVENT_NAME,

            "occurredAt": now.isoformat(),

            "order": {
                "orderId": order_id,
                "warehouseId": warehouse_id,
                "status": "RESERVED"
            },

            "reservations": reservations,

            "correlationId": correlation_id
        }

        # =====================================================
        # 12. OUTBOX
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
        # 13. LOG
        # =====================================================

        log_event_success(
            EVENT_NAME,
            {
                "order_id": order_id,
                "warehouse_id": warehouse_id,
                "allocation_count": len(reservations),
                "correlation_id": correlation_id
            }
        )

        return {
            "order_id": order_id,
            "reservation_count": len(reservations),
            "reservations": reservations
        }


# =========================================================
# SCRIPT ENTRY POINT
# =========================================================

if __name__ == "__main__":

    try:

        if len(sys.argv) < 2:

            raise Exception(
                """
Missing order ID.

Usage:

python inventory_reserved.py ORD-000000001
"""
            )

        order_id = sys.argv[1]

        generate_inventory_reserved(
            order_id
        )

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise