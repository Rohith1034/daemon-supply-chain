from datetime import datetime, timezone
import uuid


from core.db import Database
from core.ids import next_allocation_id
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "InventoryAllocationCreated"



def generate_inventory_allocation_created(order_id):


    with Database() as db:


        # --------------------------------------------------
        # Validate order exists
        # --------------------------------------------------

        order = db.fetch_one(

            """

            SELECT

                order_id,
                warehouse_id,
                correlation_id


            FROM orders


            WHERE order_id=%s


            """,

            (
                order_id,
            )

        )


        if not order:

            raise Exception(

                f"Order not found {order_id}"

            )



        warehouse_id = order["warehouse_id"]


        correlation_id = (

            str(order["correlation_id"])

            if order["correlation_id"]

            else str(uuid.uuid4())

        )



        # --------------------------------------------------
        # Fetch all order items
        # --------------------------------------------------

        order_items = db.fetch_all(

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



        if not order_items:


            raise Exception(

                f"No order items found {order_id}"

            )



        allocations = []

        now = datetime.now(
            timezone.utc
        )



        # ==================================================
        # Allocate each product
        # ==================================================

        for item in order_items:



            product_id = item["product_id"]

            quantity = item["quantity"]



            # ----------------------------------------------
            # Duplicate allocation check
            # ----------------------------------------------

            existing = db.fetch_one(

                """

                SELECT

                    allocation_id


                FROM inventory_allocations


                WHERE order_id=%s


                AND product_id=%s


                AND allocation_status IN

                (

                    'RESERVED',

                    'ALLOCATED'

                )


                LIMIT 1


                """,

                (

                    order_id,

                    product_id

                )

            )



            if existing:

                raise Exception(

                    f"""

                    Allocation already exists

                    Product:
                    {product_id}

                    Allocation:
                    {existing['allocation_id']}

                    """

                )



            # ----------------------------------------------
            # Lock inventory
            # ----------------------------------------------

            inventory = db.fetch_one(

                """

                SELECT

                    inventory_id,

                    location_id,

                    on_hand_quantity,

                    reserved_quantity


                FROM inventory


                WHERE product_id=%s


                AND warehouse_id=%s


                FOR UPDATE


                """,

                (

                    product_id,

                    warehouse_id

                )

            )



            if not inventory:


                raise Exception(

                    f"""
                    Inventory not found

                    Product:
                    {product_id}

                    """

                )



            available_quantity = (

                inventory["on_hand_quantity"]

                -

                inventory["reserved_quantity"]

            )



            if available_quantity < quantity:


                raise Exception(

                    f"""

                    Insufficient inventory


                    Product:
                    {product_id}


                    Available:
                    {available_quantity}


                    Requested:
                    {quantity}

                    """

                )



            if not inventory["location_id"]:


                raise Exception(

                    f"""

                    Inventory location missing

                    Inventory:
                    {inventory['inventory_id']}

                    """

                )



            # ----------------------------------------------
            # Generate allocation id
            # ----------------------------------------------

            allocation_id = next_allocation_id(db)



            # ----------------------------------------------
            # Update reserved quantity
            # ----------------------------------------------

            db.execute(

                """

                UPDATE inventory


                SET


                    reserved_quantity =

                    reserved_quantity + %s,


                    last_updated_at=%s


                WHERE inventory_id=%s


                """,

                (

                    quantity,

                    now,

                    inventory["inventory_id"]

                )

            )



            # ----------------------------------------------
            # Insert allocation
            # ----------------------------------------------

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

                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s

                )


                """,

                (

                    allocation_id,

                    order_id,

                    warehouse_id,

                    product_id,

                    quantity,

                    "RESERVED",

                    now,

                    correlation_id,

                    inventory["inventory_id"],

                    inventory["location_id"]

                )

            )



            allocations.append(

                {

                    "allocationId": allocation_id,

                    "productId": product_id,

                    "quantity": quantity,

                    "inventoryId": inventory["inventory_id"],

                    "locationId": inventory["location_id"]

                }

            )

            db.execute(
                """
                UPDATE orders
                SET
                    order_status='ALLOCATED'
                WHERE order_id=%s
                """,
                (
                    order_id,
                )
            )



        # --------------------------------------------------
        # Event payload
        # --------------------------------------------------

        payload = {


            "eventType":

                EVENT_NAME,


            "occurredAt":

                now.isoformat(),


            "orderId":

                order_id,


            "warehouseId":

                warehouse_id,


            "allocations":

                allocations,


            "status":

                "RESERVED",


            "correlationId":

                correlation_id


        }



        # --------------------------------------------------
        # Outbox
        # --------------------------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="INVENTORY_ALLOCATION",

            aggregate_id=order_id,

            correlation_id=correlation_id,

            payload=payload

        )



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

        order_id = sys.argv[1]


        generate_inventory_allocation_created(
            order_id
        )


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise