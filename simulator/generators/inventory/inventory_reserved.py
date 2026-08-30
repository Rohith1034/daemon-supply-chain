from datetime import datetime, timezone

import uuid

from core.db import Database
from core.ids import next_allocation_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

EVENT_NAME = "InventoryReserved"


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

        if order["order_status"] != "ALLOCATED":
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

        now = datetime.now(
            timezone.utc
        )

        allocations = []

        # =====================================================
        # 3. PROCESS EACH ORDER ITEM
        # =====================================================

        for item in items:

            product_id = item["product_id"]

            quantity = item["quantity"]

            # -------------------------------------------------
            # Check duplicate allocation
            # -------------------------------------------------

            existing = db.fetch_one(
                """
                SELECT
                    allocation_id,
                    allocation_status
                FROM inventory_allocations
                WHERE order_id=%s
                AND product_id=%s
                AND allocation_status IN
                (
                    'ALLOCATED',
                    'RESERVED'
                )
                LIMIT 1
                """,
                (
                    order_id,
                    product_id
                )
            )

            if existing:
                allocations.append(
                    {
                        "allocationId":
                            existing["allocation_id"],

                        "productId":
                            product_id,

                        "quantity":
                            quantity,

                        "status":
                            existing["allocation_status"]
                    }
                )

                continue

            # =================================================
            # 4. FIND INVENTORY
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
                    damaged_quantity
                FROM inventory
                WHERE product_id=%s
                AND warehouse_id=%s
                FOR UPDATE
                LIMIT 1
                """,
                (
                    product_id,
                    warehouse_id
                )
            )

            if not inventory:
                raise Exception(
                    f"""
Inventory not found.

PRODUCT:
{product_id}

WAREHOUSE:
{warehouse_id}
"""
                )

            available_quantity = (

                    inventory["on_hand_quantity"]

                    -
                    inventory["reserved_quantity"]

                    -
                    inventory["damaged_quantity"]

            )

            if available_quantity < quantity:
                raise Exception(
                    f"""
Insufficient inventory.

PRODUCT:
{product_id}

AVAILABLE:
{available_quantity}

REQUIRED:
{quantity}
"""
                )

            # =================================================
            # 5. CREATE ALLOCATION
            # =================================================

            allocation_id = next_allocation_id(
                db
            )

            db.execute(
                """
                INSERT INTO inventory_allocations
                (
                    allocation_id,

                    order_id,

                    warehouse_id,

                    product_id,

                    allocated_quantity,

                    allocation_status,

                    allocated_at,

                    correlation_id,

                    inventory_id,

                    location_id
                )

                VALUES
                (
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s
                )

                """,
                (

                    allocation_id,

                    order_id,

                    warehouse_id,

                    product_id,

                    quantity,

                    "ALLOCATED",

                    now,

                    correlation_id,

                    inventory["inventory_id"],

                    inventory["location_id"]

                )
            )

            allocations.append(
                {

                    "allocationId":
                        allocation_id,

                    "productId":
                        product_id,

                    "inventoryId":
                        inventory["inventory_id"],

                    "locationId":
                        inventory["location_id"],

                    "quantity":
                        quantity,

                    "status":
                        "ALLOCATED"

                }
            )

        # =====================================================
        # 6. UPDATE ORDER
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
        # 7. EVENT PAYLOAD
        # =====================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                now.isoformat(),

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
                allocations,

            "correlationId":
                correlation_id

        }

        # =====================================================
        # 8. OUTBOX EVENT
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
        # 9. LOG
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

                "correlation_id":
                    correlation_id

            }

        )

        return {

            "order_id":
                order_id,

            "allocations":
                allocations

        }


if __name__ == "__main__":

    import sys

    try:

        if len(sys.argv) < 2:
            raise Exception(
                """
Missing order id.

Usage:

python inventory_allocation_created.py ORD-000000001
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
