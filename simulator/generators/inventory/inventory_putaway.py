from datetime import timezone
import sys

from core.db import Database
from core.logger import (
    log_event_failure,
    log_event_success
)
from core.outbox import publish_event
from core.simulation_clock import get_simulation_now


EVENT_NAME = "InventoryPutaway"


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
# FIND STORAGE LOCATION
# ============================================================

def _find_storage_location(
        db,
        warehouse_id,
        quantity
):

    location = db.fetch_one(
        """
        SELECT

            location_id,

            warehouse_id,

            capacity_units,

            current_utilization,

            storage_type


        FROM warehouse_locations


        WHERE warehouse_id=%s


        AND status='ACTIVE'


        AND capacity_units IS NOT NULL


        AND (
            capacity_units -
            current_utilization
        ) >= %s


        ORDER BY current_utilization ASC


        LIMIT 1


        FOR UPDATE

        """,
        (
            warehouse_id,
            quantity
        )
    )


    if not location:

        raise Exception(
            f"""
No storage location available


WAREHOUSE :
{warehouse_id}


REQUIRED QTY :
{quantity}

"""
        )


    return location



# ============================================================
# MAIN EVENT
# ============================================================

def generate_inventory_putaway(
        inventory_id=None
):


    with Database() as db:


        print(
f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}


SEARCHING STOCK READY FOR PUTAWAY

============================================================
"""
        )



        # ====================================================
        # INVENTORY ID REQUIRED
        # ====================================================


        if not inventory_id:

            raise Exception(
                "inventory_id required for InventoryPutaway"
            )



        # ====================================================
        # FIND INVENTORY
        # ====================================================


        inventory = db.fetch_one(
            """

            SELECT

                inventory_id,

                product_id,

                warehouse_id,

                on_hand_quantity,

                available_quantity,

                reserved_quantity,

                damaged_quantity,

                location_id,

                inventory_status,

                correlation_id,

                last_updated_at


            FROM inventory


            WHERE inventory_id=%s


            AND inventory_status='AVAILABLE'


            AND location_id IS NULL


            LIMIT 1


            FOR UPDATE


            """,
            (
                inventory_id,
            )
        )



        if not inventory:


            raise Exception(
f"""
No inventory ready for putaway


INVENTORY ID :
{inventory_id}

"""
            )



        inventory_id = inventory["inventory_id"]

        product_id = inventory["product_id"]

        warehouse_id = inventory["warehouse_id"]

        quantity = inventory["available_quantity"]


        correlation_id = str(
            inventory["correlation_id"]
        )



        print(
f"""
============================================================

INVENTORY FOUND


INVENTORY ID :
{inventory_id}


PRODUCT :
{product_id}


WAREHOUSE :
{warehouse_id}


AVAILABLE QTY :
{quantity}


STATUS :
{inventory["inventory_status"]}


============================================================
"""
        )



        if quantity <= 0:

            raise Exception(
                f"Invalid available quantity {quantity}"
            )



        # ====================================================
        # FIND LOCATION
        # ====================================================


        location = _find_storage_location(
            db,
            warehouse_id,
            quantity
        )


        location_id = location["location_id"]



        print(
f"""
============================================================

LOCATION FOUND


LOCATION :
{location_id}


CAPACITY :
{location["capacity_units"]}


CURRENT :
{location["current_utilization"]}


============================================================
"""
        )



        simulation_now = _ensure_utc(
            get_simulation_now()
        )



        # ====================================================
        # UPDATE INVENTORY
        # ====================================================


        db.execute(
            """

            UPDATE inventory


            SET


                location_id=%s,


                inventory_status=%s,


                last_updated_at=%s



            WHERE inventory_id=%s



            """,
            (

                location_id,

                "AVAILABLE",

                simulation_now,

                inventory_id

            )
        )



        # ====================================================
        # UPDATE LOCATION UTILIZATION
        # ====================================================


        db.execute(
            """

            UPDATE warehouse_locations


            SET


                current_utilization =
                current_utilization + %s



            WHERE location_id=%s



            """,
            (

                quantity,

                location_id

            )
        )



        # ====================================================
        # INVENTORY TRANSACTION
        # ====================================================


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

            (%s,%s,%s,%s,%s,%s,%s,%s)

            """,
            (

                inventory_id,

                product_id,

                warehouse_id,

                "AVAILABLE",

                quantity,

                "INVENTORY",

                str(inventory_id),

                correlation_id

            )
        )



        print(
"""
============================================================

INVENTORY TRANSACTION CREATED


TYPE :
PUTAWAY_COMPLETED


============================================================
"""
        )



        # ====================================================
        # EVENT PAYLOAD
        # ====================================================


        payload = {


            "event_type":
                EVENT_NAME,


            "occurred_at":
                simulation_now.isoformat(),


            "inventory":

            {

                "inventory_id":
                    inventory_id,


                "product_id":
                    product_id,


                "warehouse_id":
                    warehouse_id,


                "location_id":
                    location_id,


                "quantity":
                    quantity,


                "status":
                    "AVAILABLE"

            },


            "putaway":

            {

                "completed":
                    True,


                "location_id":
                    location_id

            },


            "correlation_id":
                correlation_id


        }



        # ====================================================
        # OUTBOX
        # ====================================================


        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="INVENTORY",

            aggregate_id=str(inventory_id),

            correlation_id=correlation_id,

            payload=payload

        )



        # ====================================================
        # SUCCESS LOG
        # ====================================================


        log_event_success(

            EVENT_NAME,

            {

                "inventory_id":
                    inventory_id,


                "product_id":
                    product_id,


                "warehouse_id":
                    warehouse_id,


                "location_id":
                    location_id,


                "quantity":
                    quantity

            }

        )



        print(
f"""
============================================================

EVENT : {EVENT_NAME}


STATUS :
SUCCESS


INVENTORY :
{inventory_id}


LOCATION :
{location_id}


QUANTITY :
{quantity}


============================================================
"""
        )



        return {


            "inventory_id":
                inventory_id,


            "product_id":
                product_id,


            "warehouse_id":
                warehouse_id,


            "location_id":
                location_id,


            "quantity":
                quantity,


            "status":
                "AVAILABLE"

        }



# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:


        if len(sys.argv) > 1:


            generate_inventory_putaway(
                sys.argv[1]
            )


        else:


            generate_inventory_putaway()



    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise