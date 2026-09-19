from datetime import timezone

from core.db import Database
from core.outbox import publish_event
from core.logger import (
    log_event_success,
    log_event_failure
)
from core.simulation_clock import get_simulation_now


EVENT_NAME = "InventoryReserved"



# ============================================================
# DATETIME NORMALIZER
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



# ============================================================
# MAIN EVENT
# ============================================================

def generate_inventory_reserved(
        order_id=None
):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

SEARCHING ALLOCATED INVENTORY

============================================================
"""
        )


        # ====================================================
        # FIND ALLOCATED INVENTORY
        # ====================================================


        if order_id:


            allocations = db.fetch_all(
                """
                SELECT
                    allocation_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    allocated_quantity,
                    inventory_id,
                    location_id,
                    correlation_id
                FROM inventory_allocations
                WHERE order_id=%s
                  AND allocation_status='ALLOCATED'
                ORDER BY allocation_id
                """,
                (
                    order_id,
                )
            )


        else:


            allocations = db.fetch_all(
                """
                SELECT
                    allocation_id,
                    order_id,
                    warehouse_id,
                    product_id,
                    allocated_quantity,
                    inventory_id,
                    location_id,
                    correlation_id
                FROM inventory_allocations
                WHERE allocation_status='ALLOCATED'
                ORDER BY allocated_at
                """
            )



        if not allocations:


            raise Exception(
                "No ALLOCATED inventory found"
            )



        order_id = allocations[0]["order_id"]

        warehouse_id = allocations[0]["warehouse_id"]

        correlation_id = str(
            allocations[0]["correlation_id"]
        )



        print(
            f"""
============================================================
ALLOCATIONS FOUND

ORDER ID:
{order_id}

WAREHOUSE:
{warehouse_id}

COUNT:
{len(allocations)}

============================================================
"""
        )



        reserved_items = []



        # ====================================================
        # RESERVE EACH INVENTORY
        # ====================================================


        for allocation in allocations:


            inventory = db.fetch_one(
                """
                SELECT
                    inventory_id,
                    available_quantity,
                    reserved_quantity,
                    inventory_status
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
Inventory not found

ID:
{allocation["inventory_id"]}
"""
                )



            qty = allocation[
                "allocated_quantity"
            ]



            available_qty = inventory[
                "available_quantity"
            ]



            if available_qty < qty:


                raise Exception(
                    f"""
Insufficient stock

Inventory:
{allocation["inventory_id"]}

Available:
{available_qty}

Required:
{qty}
"""
                )



            # --------------------------------------------
            # Update inventory
            # --------------------------------------------

            db.execute(
                """
                UPDATE inventory
                SET

                    reserved_quantity =
                        reserved_quantity + %s,

                    inventory_status=%s,

                    last_updated_at=%s

                WHERE inventory_id=%s
                """,
                (
                    qty,

                    "AVAILABLE",

                    _ensure_utc(
                        get_simulation_now()
                    ),

                    allocation["inventory_id"]
                )
            )



            # --------------------------------------------
            # Update allocation
            # --------------------------------------------

            db.execute(
                """
                UPDATE inventory_allocations
                SET
                    allocation_status='RESERVED'
                WHERE allocation_id=%s
                """,
                (
                    allocation["allocation_id"],
                )
            )



            # --------------------------------------------
            # Inventory transaction
            # --------------------------------------------


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
                    allocation["inventory_id"],
                    allocation["product_id"],
                    allocation["warehouse_id"],
                    "STOCK_RESERVED",
                    qty,
                    "ORDER",
                    order_id,
                    correlation_id
                )
            )



            reserved_items.append(
                {

                    "allocation_id":
                        allocation["allocation_id"],

                    "inventory_id":
                        allocation["inventory_id"],

                    "product_id":
                        allocation["product_id"],

                    "location_id":
                        allocation["location_id"],

                    "reserved_quantity":
                        qty

                }
            )



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================


        reserved_at = _ensure_utc(
            get_simulation_now()
        )



        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                reserved_at.isoformat(),



            "reservation":

            {

                "order_id":
                    order_id,


                "warehouse_id":
                    warehouse_id,


                "items":
                    reserved_items,


                "status":
                    "RESERVED"

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

                "reserved_items":
                    len(reserved_items)

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

RESERVED ITEMS:
{len(reserved_items)}

============================================================
"""
        )


        return {

            "order_id":
                order_id,

            "items":
                reserved_items

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:

        generate_inventory_reserved()


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise