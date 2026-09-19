from datetime import timezone
import sys

from core.db import Database
from core.logger import (
    log_event_failure,
    log_event_success
)
from core.outbox import publish_event
from core.simulation_clock import get_simulation_now


EVENT_NAME = "StockIncreased"


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

def generate_stock_increased(inventory_id=None):


    with Database() as db:


        print(
            f"""
============================================================
PROCESSING EVENT : {EVENT_NAME}

SEARCHING RECEIVED INVENTORY

============================================================
"""
        )



        # ====================================================
        # FIND RECEIVED INVENTORY
        # ====================================================


        if not inventory_id:

            raise Exception(
                "inventory_id is required for StockIncreased event"
            )



        inventory = db.fetch_one(
            """
            SELECT

                inventory_id,

                product_id,

                warehouse_id,

                on_hand_quantity,

                reserved_quantity,

                damaged_quantity,

                available_quantity,

                location_id,

                correlation_id,

                inventory_status,

                last_updated_at


            FROM inventory


            WHERE inventory_id=%s


            AND inventory_status='RECEIVED'


            LIMIT 1

            """,
            (
                inventory_id,
            )
        )



        if not inventory:

            raise Exception(
                f"""
No RECEIVED inventory found

INVENTORY ID :
{inventory_id}

"""
            )



        inventory_id = inventory["inventory_id"]

        product_id = inventory["product_id"]

        warehouse_id = inventory["warehouse_id"]

        on_hand_quantity = inventory["on_hand_quantity"]

        reserved_quantity = (
            inventory["reserved_quantity"]
            or 0
        )

        damaged_quantity = (
            inventory["damaged_quantity"]
            or 0
        )

        correlation_id = str(
            inventory["correlation_id"]
        )



        print(
            f"""
============================================================
INVENTORY FOUND


INVENTORY ID :
{inventory_id}


PRODUCT ID :
{product_id}


WAREHOUSE :
{warehouse_id}


ON HAND :
{on_hand_quantity}


RESERVED :
{reserved_quantity}


DAMAGED :
{damaged_quantity}


STATUS :
{inventory["inventory_status"]}


============================================================
"""
        )



        # ====================================================
        # VALIDATION
        # ====================================================


        if on_hand_quantity <= 0:

            raise Exception(
                f"""
Invalid on hand quantity

VALUE :
{on_hand_quantity}

"""
            )



        # ====================================================
        # CALCULATE AVAILABLE
        # ====================================================


        available_quantity = (

            on_hand_quantity

            -

            reserved_quantity

            -

            damaged_quantity

        )



        if available_quantity < 0:

            raise Exception(
                f"""
Invalid stock calculation


ON HAND :
{on_hand_quantity}


RESERVED :
{reserved_quantity}


DAMAGED :
{damaged_quantity}


AVAILABLE :
{available_quantity}

"""
            )



        simulation_now = _ensure_utc(
            get_simulation_now()
        )



        print(
            f"""
============================================================
STOCK CALCULATION


ON HAND :
{on_hand_quantity}


AVAILABLE :
{available_quantity}


TIME :
{simulation_now}


============================================================
"""
        )



        # ====================================================
        # UPDATE INVENTORY
        # ====================================================

        # ====================================================
        # UPDATE INVENTORY
        # ====================================================

        db.execute(
            """
            UPDATE inventory

            SET

                inventory_status=%s,

                last_updated_at=%s


            WHERE inventory_id=%s

            """,
            (

                "AVAILABLE",

                simulation_now,

                inventory_id

            )
        )



        print(
            f"""
============================================================
INVENTORY UPDATED


INVENTORY ID :
{inventory_id}


STATUS :
AVAILABLE


AVAILABLE QTY :
{available_quantity}


LOCATION :
NULL


============================================================
"""
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

            (

                %s,%s,%s,%s,%s,%s,%s,%s

            )

            """,
            (

                inventory_id,

                product_id,

                warehouse_id,

                "STOCK_INCREASED",

                on_hand_quantity,

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
STOCK_INCREASED


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


                "on_hand_quantity":
                    on_hand_quantity,


                "available_quantity":
                    available_quantity,


                "status":
                    "AVAILABLE",


                "location_id":
                    None,


                "next_event":
                    "InventoryPutaway"

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

            aggregate_type="INVENTORY",

            aggregate_id=str(inventory_id),

            correlation_id=correlation_id,

            payload=payload

        )



        print(
            f"""
============================================================
OUTBOX EVENT CREATED


EVENT :
{EVENT_NAME}


INVENTORY ID :
{inventory_id}


============================================================
"""
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


                "available_quantity":
                    available_quantity

            }

        )



        print(
            f"""
============================================================
EVENT : {EVENT_NAME}

INVENTORY ID :
{inventory_id}

PRODUCT :
{product_id}

AVAILABLE QUANTITY :
{available_quantity}

STATUS :
SUCCESS

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


            "available_quantity":
                available_quantity,


            "status":
                "AVAILABLE"

        }




# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":


    try:


        if len(sys.argv) > 1:


            generate_stock_increased(
                sys.argv[1]
            )


        else:


            generate_stock_increased()



    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise