import random
import string
from datetime import timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "InventoryAllocationCreated"



# ============================================================
# HELPERS
# ============================================================

def _ensure_utc(value):

    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )



def _generate_allocation_id():

    suffix = ''.join(
        random.choices(
            string.ascii_uppercase +
            string.digits,
            k=6
        )
    )

    return f"ALLOC-{suffix}"



# ============================================================
# FIND AVAILABLE INVENTORY
# ============================================================

def _find_available_inventory(
        db,
        warehouse_id,
        product_id
):

    return db.fetch_all(
        """
        SELECT
            inventory_id,
            product_id,
            warehouse_id,
            available_quantity,
            location_id
        FROM inventory
        WHERE warehouse_id=%s
          AND product_id=%s
          AND inventory_status='AVAILABLE'
          AND available_quantity > 0
          AND location_id IS NOT NULL
        ORDER BY available_quantity DESC
        FOR UPDATE
        """,
        (
            warehouse_id,
            product_id
        )
    )



# ============================================================
# CHECK EXISTING ALLOCATION
# ============================================================

def _check_existing_allocation(
        db,
        order_id,
        product_id
):

    return db.fetch_one(
        """
        SELECT
            allocation_id
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



# ============================================================
# MAIN EVENT
# ============================================================

def generate_inventory_allocation_created(
        order_id=None
):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

CREATING INVENTORY ALLOCATION

============================================================
"""
        )



        # ====================================================
        # FIND ORDER
        # ====================================================

        if order_id:


            order = db.fetch_one(
                """
                SELECT
                    o.order_id,
                    o.warehouse_id,
                    o.correlation_id
                FROM orders o
                WHERE o.order_id=%s
                  AND o.items_created=true
                  AND o.order_status='CREATED'
                  AND NOT EXISTS
                  (
                      SELECT 1
                      FROM inventory_allocations ia
                      WHERE ia.order_id=o.order_id
                        AND ia.allocation_status IN
                        (
                            'ALLOCATED',
                            'RESERVED'
                        )
                  )
                LIMIT 1
                FOR UPDATE
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
                    o.warehouse_id,
                    o.correlation_id
                FROM orders o
                WHERE o.items_created=true
                  AND o.order_status='CREATED'
                  AND NOT EXISTS
                  (
                      SELECT 1
                      FROM inventory_allocations ia
                      WHERE ia.order_id=o.order_id
                        AND ia.allocation_status IN
                        (
                            'ALLOCATED',
                            'RESERVED'
                        )
                  )
                ORDER BY o.created_at
                LIMIT 1
                FOR UPDATE
                """
            )



        if not order:

            raise Exception(
                "No CREATED order available for allocation"
            )



        order_id = order["order_id"]

        warehouse_id = order["warehouse_id"]

        correlation_id = str(
            order["correlation_id"]
        )



        print(
            f"""
============================================================
ORDER FOUND

ORDER ID       : {order_id}
WAREHOUSE      : {warehouse_id}
CORRELATION ID : {correlation_id}

============================================================
"""
        )



        # ====================================================
        # FETCH ORDER ITEMS
        # ====================================================

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
                f"No order items found for {order_id}"
            )



        allocations = []



        # ====================================================
        # PROCESS EACH PRODUCT
        # ====================================================

        for item in items:


            product_id = item["product_id"]

            required_qty = item["quantity"]



            print(
                f"""
------------------------------------------------------------
PRODUCT

PRODUCT ID :
{product_id}

REQUIRED QTY :
{required_qty}

------------------------------------------------------------
"""
            )



            existing = _check_existing_allocation(
                db,
                order_id,
                product_id
            )


            if existing:

                raise Exception(
                    f"""
Allocation already exists

ORDER:
{order_id}

PRODUCT:
{product_id}

ALLOCATION:
{existing["allocation_id"]}
"""
                )



            inventory_rows = _find_available_inventory(
                db,
                warehouse_id,
                product_id
            )



            if not inventory_rows:

                raise Exception(
                    f"""
No inventory found

PRODUCT:
{product_id}

WAREHOUSE:
{warehouse_id}
"""
                )



            remaining_qty = required_qty



            # =================================================
            # SPLIT ACROSS LOCATIONS
            # =================================================

            for inventory in inventory_rows:


                if remaining_qty <= 0:

                    break



                available_qty = inventory[
                    "available_quantity"
                ]


                allocated_qty = min(
                    available_qty,
                    remaining_qty
                )



                allocation_id = _generate_allocation_id()
                # =================================================
                # CREATE INVENTORY ALLOCATION
                # =================================================

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
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s
                    )
                    """,
                    (
                        allocation_id,
                        order_id,
                        warehouse_id,
                        product_id,
                        allocated_qty,
                        "ALLOCATED",
                        _ensure_utc(
                            get_simulation_now()
                        ),
                        correlation_id,
                        inventory["inventory_id"],
                        inventory["location_id"]
                    )
                )

                allocations.append(
                    {

                        "allocation_id":
                            allocation_id,

                        "product_id":
                            product_id,

                        "inventory_id":
                            inventory["inventory_id"],

                        "location_id":
                            inventory["location_id"],

                        "allocated_quantity":
                            allocated_qty,

                        "allocation_status":
                            "ALLOCATED"

                    }
                )

                remaining_qty -= allocated_qty

                # =================================================
                # VALIDATE FULL ALLOCATION
                # =================================================

            if remaining_qty > 0:
                raise Exception(
                    f"""
                Insufficient inventory

                ORDER:
                {order_id}

                PRODUCT:
                {product_id}

                MISSING QUANTITY:
                {remaining_qty}
                """
                )

                # ====================================================
                # EVENT TIME
                # ====================================================

            allocation_time = _ensure_utc(
                get_simulation_now()
            )

            # ====================================================
            # EVENT PAYLOAD
            # ====================================================

            payload = {

                "event_type":
                    EVENT_NAME,

                "occurred_at":
                    allocation_time.isoformat(),

                "allocation":

                    {

                        "order_id":
                            order_id,

                        "warehouse_id":
                            warehouse_id,

                        "allocations":
                            allocations,

                        "total_allocations":
                            len(allocations)

                    },

                "correlation_id":
                    correlation_id

            }

            # ====================================================
            # OUTBOX EVENT
            # ====================================================

            publish_event(
                db=db,
                event_type=EVENT_NAME,
                aggregate_type="ORDER",
                aggregate_id=order_id,
                correlation_id=correlation_id,
                payload=payload
            )

            # ====================================================
            # SUCCESS LOG
            # ====================================================

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

            print(
                f"""
                ============================================================
                EVENT SUCCESS

                EVENT:
                {EVENT_NAME}

                ORDER:
                {order_id}

                ALLOCATIONS:
                {len(allocations)}

                STATUS:
                ALLOCATED

                ============================================================
                """
            )

            return {

                "order_id":
                    order_id,

                "allocations":
                    allocations,

                "status":
                    "ALLOCATED"

            }

        # ============================================================
        # MAIN
        # ============================================================

if __name__ == "__main__":

    try:
        generate_inventory_allocation_created()
    except Exception as e:

        log_event_failure(
                    EVENT_NAME,
                    e
                )

        raise