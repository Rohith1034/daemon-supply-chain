from datetime import timedelta, timezone
import random
import sys

from core.db import Database
from core.logger import (
    log_event_failure,
    log_event_success
)
from core.outbox import publish_event
from core.simulation_clock import get_simulation_now


EVENT_NAME = "GoodsReceived"


# ============================================================
# UTC NORMALIZER
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
# GOODS RECEIVED TIME
# ============================================================

def _get_goods_received_time(task):

    task_started_at = _ensure_utc(
        task["task_started_at"]
    )

    assigned_at = _ensure_utc(
        task["assigned_at"]
    )

    created_at = _ensure_utc(
        task["created_at"]
    )


    if task_started_at:
        base_time = task_started_at

    elif assigned_at:
        base_time = assigned_at

    elif created_at:
        base_time = created_at

    else:
        base_time = _ensure_utc(
            get_simulation_now()
        )


    minutes = random.randint(
        10,
        45
    )


    return (
        base_time + timedelta(minutes=minutes),
        minutes
    )



# ============================================================
# MAIN EVENT
# ============================================================

def generate_goods_received(task_id=None):


    with Database() as db:


        print(
"""
============================================================
PROCESSING EVENT : GoodsReceived
============================================================
"""
        )


        # ====================================================
        # FIND RECEIVING TASK
        # ====================================================


        if task_id:


            task = db.fetch_one(
"""
SELECT

    task_id,
    shipment_id,
    warehouse_id,
    assigned_worker_id,
    expected_quantity,
    quantity,
    task_started_at,
    assigned_at,
    created_at,
    correlation_id

FROM warehouse_tasks

WHERE task_id=%s

AND task_type='RECEIVING'

AND status='STARTED'

LIMIT 1

""",
(
task_id,
)

)


        else:


            task = db.fetch_one(
"""
SELECT

    task_id,
    shipment_id,
    warehouse_id,
    assigned_worker_id,
    expected_quantity,
    quantity,
    task_started_at,
    assigned_at,
    created_at,
    correlation_id

FROM warehouse_tasks

WHERE task_type='RECEIVING'

AND status='CREATED'

ORDER BY created_at DESC

LIMIT 1

"""
)



        if not task:

            raise Exception(
                "No STARTED receiving task found"
            )


        task_id = task["task_id"]

        shipment_id = task["shipment_id"]

        warehouse_id = task["warehouse_id"]

        worker_id = task["assigned_worker_id"]

        correlation_id = str(
            task["correlation_id"]
        )


        expected_quantity = (
            task["expected_quantity"]
            or
            task["quantity"]
        )



        received_at, actual_minutes = (
            _get_goods_received_time(task)
        )



        print(
f"""
============================================================
RECEIVING TASK FOUND

TASK ID :
{task_id}

SHIPMENT ID :
{shipment_id}

WAREHOUSE ID :
{warehouse_id}

EXPECTED QTY :
{expected_quantity}

RECEIVED TIME :
{received_at}

============================================================
"""
)



        # ====================================================
        # FETCH SHIPMENT ITEMS
        # ====================================================


        items = db.fetch_all(
"""
SELECT

    shipment_item_id,
    product_id,
    shipped_quantity

FROM shipment_items

WHERE shipment_id=%s

ORDER BY shipment_item_id

""",
(
shipment_id,
)

)



        if not items:

            raise Exception(
                "No shipment items found"
            )



        total_received_quantity = sum(
            i["shipped_quantity"]
            for i in items
        )



        if expected_quantity != total_received_quantity:

            raise Exception(
f"""
Quantity mismatch

Expected:
{expected_quantity}

Found:
{total_received_quantity}

"""
            )



        created_inventory = []



        # ====================================================
        # PROCESS EACH PRODUCT
        # ====================================================


        for item in items:


            product_id = item["product_id"]

            quantity = item["shipped_quantity"]



            print(
f"""
------------------------------------------------------------
PROCESSING PRODUCT

PRODUCT :
{product_id}

QTY :
{quantity}

------------------------------------------------------------
"""
)



            # Update shipment item

            db.execute(
"""
UPDATE shipment_items

SET received_quantity=%s

WHERE shipment_item_id=%s

""",
(
quantity,
item["shipment_item_id"]
)

)




            # Check inventory


            inventory = db.fetch_one(
"""
SELECT

    inventory_id

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




            # ====================================================
            # CREATE INVENTORY
            # ====================================================


            if not inventory:


                inventory = db.fetch_one(
"""
INSERT INTO inventory
(
product_id,
warehouse_id,
on_hand_quantity,
reserved_quantity,
damaged_quantity,
location_id,
inventory_status,
correlation_id,
last_updated_at
)

VALUES
(
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s
)

RETURNING inventory_id

""",
(
product_id,
warehouse_id,
quantity,
0,
0,
None,
"RECEIVED",
correlation_id,
received_at
)

)



                inventory_id = inventory["inventory_id"]



            else:


                inventory_id = inventory["inventory_id"]


                db.execute(
"""
UPDATE inventory

SET

on_hand_quantity =
on_hand_quantity + %s,

inventory_status='RECEIVED',

last_updated_at=%s


WHERE inventory_id=%s

""",
(
quantity,
received_at,
inventory_id
)

)



            created_inventory.append(
{
"inventory_id":inventory_id,
"product_id":product_id,
"quantity":quantity
}
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
task_id,
shipment_id,
correlation_id
)

VALUES
(
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)

""",
(
inventory_id,
product_id,
warehouse_id,
"GOODS_RECEIVED",
quantity,
"SHIPMENT",
shipment_id,
task_id,
shipment_id,
correlation_id
)

)




        # ====================================================
        # COMPLETE TASK
        # ====================================================


        db.execute(
"""
UPDATE warehouse_tasks

SET

status='COMPLETED',

received_quantity=%s,

actual_minutes=%s,

completed_at=%s,

task_completed_at=%s,

completed_by=%s


WHERE task_id=%s

""",
(
total_received_quantity,
actual_minutes,
received_at,
received_at,
worker_id,
task_id
)

)



        # Release worker

        if worker_id:


            db.execute(
"""
UPDATE workers

SET current_status='AVAILABLE'

WHERE worker_id=%s

""",
(
worker_id,
)

)



        # Update shipment


        db.execute(
"""
UPDATE shipments

SET

shipment_status='RECEIVED',

updated_at=%s


WHERE shipment_id=%s

""",
(
received_at,
shipment_id
)

)




        # ====================================================
        # EVENT PAYLOAD
        # ====================================================


        payload = {


"event_type":EVENT_NAME,


"occurred_at":
received_at.isoformat(),


"goods_received":
{

"task_id":task_id,

"shipment_id":shipment_id,

"warehouse_id":warehouse_id,

"worker_id":worker_id,

"received_quantity":
total_received_quantity,

"status":"RECEIVED"

},


"inventory":
{

"items":created_inventory,

"next_event":
"StockIncreased"

},


"correlation_id":
correlation_id


}




        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="SHIPMENT",
            aggregate_id=shipment_id,
            correlation_id=correlation_id,
            payload=payload
        )



        log_event_success(
            EVENT_NAME,
            {
            "task_id":task_id,
            "shipment_id":shipment_id,
            "quantity":total_received_quantity
            }
        )



        print(
f"""
============================================================
GOODS RECEIVED SUCCESS

TASK :
{task_id}

SHIPMENT :
{shipment_id}

TOTAL QTY :
{total_received_quantity}

NEXT EVENT :
StockIncreased

============================================================
"""
)



        return {

            "task_id":task_id,

            "shipment_id":shipment_id,

            "received_quantity":
            total_received_quantity,

            "inventory":
            created_inventory

        }



# ============================================================
# RUN
# ============================================================


if __name__ == "__main__":


    try:


        if len(sys.argv)>1:

            generate_goods_received(
                sys.argv[1]
            )

        else:

            generate_goods_received()



    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise