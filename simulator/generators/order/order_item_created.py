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


def _get_confirmed_time(order):
    """
    Order confirmation must happen after the order was created.

    The order_date is the primary business anchor. The
    simulation clock is also considered so that the generated
    timestamp remains consistent with the overall simulation
    timeline.
    """

    simulation_now = get_simulation_now()

    order_date = order.get(
        "order_date"
    )

    created_at = order.get(
        "created_at"
    )

    candidates = [
        candidate
        for candidate in [
            order_date,
            created_at,
            simulation_now
        ]
        if candidate is not None
    ]

    base_time = max(candidates)

    return (
        base_time +
        timedelta(
            minutes=random.randint(
                5,
                120
            )
        )
    )


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

        confirmed_at = _get_confirmed_time(
            order
        )

        # -------------------------------------------------
        # Select AVAILABLE inventory products
        #
        # Only inventory that has completed putaway can
        # be used for a customer order.
        # -------------------------------------------------

        products = db.fetch_all(
            """
            SELECT
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
            ORDER BY random()
            LIMIT %s
            """,
            (
                warehouse_id,
                random.randint(
                    1,
                    5
                )
            )
        )

        if not products:

            raise Exception(
                "No AVAILABLE inventory available for order"
            )

        total_quantity = 0
        total_amount = 0
        items = []

        # -------------------------------------------------
        # Create order items
        # -------------------------------------------------

        for product in products:

            available_quantity = int(
                product["available_quantity"]
            )

            if available_quantity <= 0:
                continue

            quantity = random.randint(
                1,
                min(
                    10,
                    available_quantity
                )
            )

            unit_price = float(
                product["selling_price"]
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
                    product["product_id"],
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
                        product["product_id"],

                    "quantity":
                        quantity,

                    "unitPrice":
                        unit_price,

                    "totalPrice":
                        total_price
                }
            )

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
                round(
                    total_amount,
                    2
                ),
                order_id
            )
        )

        # -------------------------------------------------
        # Confirm order
        # -------------------------------------------------
        #
        # This is still part of the current OrderItemCreated
        # business operation in your existing flow.
        #
        # Later, if you introduce a separate
        # OrderConfirmed event, this can be split out.
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
                    round(
                        total_amount,
                        2
                    ),

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
                    round(
                        total_amount,
                        2
                    ),

                "confirmed_at":
                    confirmed_at,

                "correlation_id":
                    correlation_id
            }
        )


if __name__ == "__main__":

    try:

        generate_order_item_created()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
