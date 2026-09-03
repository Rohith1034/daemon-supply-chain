from datetime import timedelta
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


EVENT_NAME = "OrderItemCreated"


# ============================================================
# TIME HELPERS
# ============================================================

def _get_confirmed_time(order):
    """
    Order confirmation must happen after the order was created.

    The order_date is the primary business anchor. The
    simulation clock is also considered so that the generated
    timestamp remains consistent with the overall simulation
    timeline.
    """

    simulation_now = get_simulation_now()

    candidates = [
        candidate
        for candidate in [
            order.get("order_date"),
            order.get("created_at"),
            simulation_now
        ]
        if candidate is not None
    ]

    base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(5, 120)
        )
    )


# ============================================================
# INVENTORY SELECTION
# ============================================================

def _fetch_available_inventory_rows(db, warehouse_id, limit):
    """
    Pull AVAILABLE stock for the order warehouse.

    The result is intentionally conservative:
    - only active products
    - only available inventory
    - only positive quantity
    - prefer higher stock rows first
    """

    return db.fetch_all(
        """
        SELECT
            i.inventory_id,
            i.product_id,
            i.available_quantity,
            p.selling_price
        FROM inventory i
        JOIN products p
            ON i.product_id = p.product_id
        WHERE i.warehouse_id=%s
          AND i.inventory_status='AVAILABLE'
          AND i.available_quantity > 0
          AND p.status='ACTIVE'
        ORDER BY i.available_quantity DESC,
                 i.product_id ASC
        LIMIT %s
        """,
        (
            warehouse_id,
            limit
        )
    )


# ============================================================
# MAIN GENERATOR
# ============================================================

def generate_order_item_created(order_id=None):

    with Database() as db:

        # -------------------------------------------------
        # Find CREATED order without items
        #
        # If order_id is provided, target that exact order.
        #
        # Otherwise preserve the existing behavior and
        # find the newest CREATED order without items.
        # -------------------------------------------------

        if order_id:

            order = db.fetch_one(
                """
                SELECT
                    o.order_id,
                    o.customer_id,
                    o.warehouse_id,
                    o.order_date,
                    o.created_at,
                    o.correlation_id
                FROM orders o
                LEFT JOIN order_items oi
                    ON o.order_id = oi.order_id
                WHERE o.order_id=%s
                  AND o.order_status='CREATED'
                  AND oi.order_id IS NULL
                LIMIT 1
                """,
                (
                    order_id,
                )
            )

        else:

            order = db.fetch_one(
                """
                SELECT
                    o.order_id,
                    o.customer_id,
                    o.warehouse_id,
                    o.order_date,
                    o.created_at,
                    o.correlation_id
                FROM orders o
                LEFT JOIN order_items oi
                    ON o.order_id = oi.order_id
                WHERE o.order_status='CREATED'
                  AND oi.order_id IS NULL
                ORDER BY o.created_at DESC
                LIMIT 1
                """
            )

        if not order:

            if order_id:

                raise Exception(
                    f"No CREATED order found without items "
                    f"for order_id={order_id}"
                )

            raise Exception(
                "No CREATED order found without items"
            )

        order_id = order["order_id"]
        warehouse_id = order["warehouse_id"]

        correlation_id = str(
            order["correlation_id"]
        )

        # -------------------------------------------------
        # Business confirmation time
        #
        # Always after OrderCreated.
        # -------------------------------------------------

        confirmed_at = _get_confirmed_time(order)

        # -------------------------------------------------
        # Pull inventory that can actually support this order
        # -------------------------------------------------

        target_item_count = random.randint(1, 4)

        inventory_rows = _fetch_available_inventory_rows(
            db=db,
            warehouse_id=warehouse_id,
            limit=target_item_count * 3
        )

        if not inventory_rows:

            raise Exception(
                f"No AVAILABLE inventory available for order {order_id} "
                f"in warehouse {warehouse_id}"
            )

        items = []
        total_quantity = 0
        total_amount = 0.0

        # -------------------------------------------------
        # Create order items from available inventory only
        # -------------------------------------------------

        for row in inventory_rows[:target_item_count]:

            available_quantity = int(
                row["available_quantity"]
            )

            if available_quantity <= 0:
                continue

            # Keep quantities conservative so allocation has
            # a higher chance of succeeding later in the flow.
            max_pick = min(5, available_quantity)
            quantity = random.randint(1, max_pick)

            unit_price = float(
                row["selling_price"]
            )

            total_price = round(
                quantity * unit_price,
                2
            )

            db.execute(
                """
                INSERT INTO order_items
                (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    total_price
                )
                VALUES
                (
                    %s,%s,%s,%s,%s
                )
                """,
                (
                    order_id,
                    row["product_id"],
                    quantity,
                    unit_price,
                    total_price
                )
            )

            total_quantity += quantity
            total_amount += total_price

            items.append(
                {
                    "productId":
                        row["product_id"],

                    "quantity":
                        quantity,

                    "unitPrice":
                        unit_price,

                    "totalPrice":
                        total_price
                }
            )

        # -------------------------------------------------
        # Fallback: if the first pass produced nothing,
        # use the best available inventory rows one by one.
        # -------------------------------------------------

        if not items:

            for row in inventory_rows:

                available_quantity = int(
                    row["available_quantity"]
                )

                if available_quantity <= 0:
                    continue

                quantity = 1

                unit_price = float(
                    row["selling_price"]
                )

                total_price = round(
                    quantity * unit_price,
                    2
                )

                db.execute(
                    """
                    INSERT INTO order_items
                    (
                        order_id,
                        product_id,
                        quantity,
                        unit_price,
                        total_price
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s
                    )
                    """,
                    (
                        order_id,
                        row["product_id"],
                        quantity,
                        unit_price,
                        total_price
                    )
                )

                total_quantity += quantity
                total_amount += total_price

                items.append(
                    {
                        "productId":
                            row["product_id"],

                        "quantity":
                            quantity,

                        "unitPrice":
                            unit_price,

                        "totalPrice":
                            total_price
                    }
                )

                if len(items) >= 1:
                    break

        # -------------------------------------------------
        # Make sure at least one item was created
        # -------------------------------------------------

        if not items:

            raise Exception(
                "No valid order items could be created"
            )

        # -------------------------------------------------
        # Update order totals
        # -------------------------------------------------

        db.execute(
            """
            UPDATE orders
            SET
                total_items=%s,
                total_quantity=%s,
                total_amount=%s
            WHERE order_id=%s
            """,
            (
                len(items),
                total_quantity,
                round(total_amount, 2),
                order_id
            )
        )

        # -------------------------------------------------
        # Confirm order
        # -------------------------------------------------
        #
        # This is still part of the current OrderItemCreated
        # business operation in your existing flow.
        # -------------------------------------------------

        db.execute(
            """
            UPDATE orders
            SET
                order_status='CONFIRMED',
                confirmed_at=%s
            WHERE order_id=%s
            """,
            (
                confirmed_at,
                order_id
            )
        )

        # -------------------------------------------------
        # Publish Event
        # -------------------------------------------------

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                confirmed_at.isoformat(),

            "order":
            {
                "orderId":
                    order_id,

                "warehouseId":
                    warehouse_id,

                "items":
                    items,

                "totalItems":
                    len(items),

                "totalQuantity":
                    total_quantity,

                "totalAmount":
                    round(total_amount, 2),

                "confirmedAt":
                    confirmed_at.isoformat()
            },

            "correlationId":
                correlation_id
        }

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="ORDER",
            aggregate_id=order_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "items":
                    len(items),

                "quantity":
                    total_quantity,

                "amount":
                    round(total_amount, 2),

                "confirmed_at":
                    confirmed_at,

                "correlation_id":
                    correlation_id
            }
        )

        return {
            "order_id":
                order_id,

            "items":
                items
        }


# ============================================================
# CLI ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        generate_order_item_created()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise