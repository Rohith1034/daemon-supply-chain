import os
import json
import subprocess
import time

from datetime import datetime

import psycopg2


# =====================================================
# PROJECT PATH
# =====================================================

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

OUTPUT_FILE = os.path.join(
    PROJECT_ROOT,
    "..",
    "output",
    "event_execution_report.json"
)

PYTHON = os.path.join(
    os.path.dirname(PROJECT_ROOT),
    ".venv",
    "Scripts",
    "python.exe"
)


# =====================================================
# DATABASE
# =====================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 55432,
    "database": "supply_chain",
    "user": "supplychain_app",
    "password": "Cr7@1034"
}


# =====================================================
# EVENT FLOW
# =====================================================

FLOW = [

    # =================================================
    # INBOUND
    # =================================================

    (
        "PurchaseOrderCreated",
        "generators/purchase_order/purchase_order_created.py"
    ),

    (
        "PurchaseOrderApproved",
        "generators/purchase_order/purchase_order_approved.py"
    ),

    (
        "SupplierShipmentCreated",
        "generators/supplier/supplier_shipment_created.py"
    ),

    (
        "ASNReceived",
        "generators/purchase_order/asn_received.py"
    ),

    (
        "SupplierShipmentDelivered",
        "generators/supplier/supplier_shipment_delivered.py"
    ),

    (
        "ReceivingTaskCreated",
        "generators/warehouse/receiving_task_created_inbound.py"
    ),

    (
        "ReceivingTaskStarted",
        "generators/warehouse/task_started.py"
    ),

    (
        "GoodsReceived",
        "generators/warehouse/goods_received.py"
    ),

    (
        "StockIncreased",
        "generators/inventory/stock_increased.py"
    ),

    (
        "InventoryPutaway",
        "generators/inventory/inventory_putaway.py"
    ),

    # =================================================
    # OUTBOUND
    # =================================================

    (
        "OrderCreated",
        "generators/order/order_created.py"
    ),

    (
        "OrderItemCreated",
        "generators/order/order_item_created.py"
    ),

    (
        "InventoryAllocationCreated",
        "generators/inventory/inventory_allocation_created.py"
    ),

    (
        "InventoryReserved",
        "generators/inventory/inventory_reserved.py"
    ),

    (
        "PickingTaskCreated",
        "generators/warehouse/picking_task_created.py"
    ),

    (
        "PickingTaskStarted",
        "generators/warehouse/task_started.py"
    ),

    (
        "PickingCompleted",
        "generators/warehouse/picking_completed.py"
    ),

    (
        "PackingTaskCreated",
        "generators/warehouse/packing_task_created.py"
    ),

    (
        "PackingTaskStarted",
        "generators/warehouse/task_started.py"
    ),

    (
        "PackingCompleted",
        "generators/warehouse/packing_completed.py"
    ),

    (
        "ShipmentReady",
        "generators/transportation/shipment_ready.py"
    ),

    (
        "CarrierAssigned",
        "generators/transportation/carrier_assigned.py"
    ),

    (
        "ShipmentPickedUp",
        "generators/transportation/shipment_picked_up.py"
    ),

    (
        "ShipmentInTransit",
        "generators/transportation/shipment_in_transit.py"
    ),

    (
        "ShipmentDelivered",
        "generators/transportation/shipment_delivered.py"
    )
]


# =====================================================
# DATABASE
# =====================================================

def get_db():

    return psycopg2.connect(
        **DB_CONFIG
    )


# =====================================================
# CURRENT ORDER
# =====================================================

def get_latest_order_id():

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT order_id
                FROM orders
                ORDER BY created_at DESC
                LIMIT 1
                """
            )

            row = cur.fetchone()

    if not row:

        raise Exception(
            "No order found"
        )

    return row[0]


# =====================================================
# GET ALLOCATION FOR EXACT ORDER
# =====================================================

def get_latest_allocation_id(order_id):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT allocation_id

                FROM inventory_allocations

                WHERE order_id=%s

                ORDER BY allocated_at DESC

                LIMIT 1
                """,
                (
                    order_id,
                )
            )

            row = cur.fetchone()

    if not row:

        raise Exception(
            f"No allocation found for order {order_id}"
        )

    return row[0]


# =====================================================
# GET PICKING TASK FOR EXACT ALLOCATION
# =====================================================

def get_picking_task_for_allocation(
    allocation_id,
    status=None
):

    with get_db() as conn:

        with conn.cursor() as cur:

            query = """
                SELECT
                    task_id
                FROM warehouse_tasks
                WHERE task_type='PICKING'
                  AND allocation_id=%s
            """

            params = [
                allocation_id
            ]

            if status:

                query += """
                    AND status=%s
                """

                params.append(
                    status
                )

            query += """
                ORDER BY created_at DESC
                LIMIT 1
            """

            cur.execute(
                query,
                params
            )

            row = cur.fetchone()

    if not row:

        raise Exception(
            f"""
No PICKING task found.

ALLOCATION:
{allocation_id}

STATUS:
{status}
"""
        )

    return row[0]


# =====================================================
# GET PACKING TASK
# =====================================================

def get_latest_packing_task_id(
    order_id=None,
    status=None
):

    with get_db() as conn:

        with conn.cursor() as cur:

            query = """
                SELECT task_id
                FROM warehouse_tasks
                WHERE task_type='PACKING'
            """

            params = []

            if order_id:

                query += """
                    AND order_id=%s
                """

                params.append(
                    order_id
                )

            if status:

                query += """
                    AND status=%s
                """

                params.append(
                    status
                )

            query += """
                ORDER BY created_at DESC
                LIMIT 1
            """

            cur.execute(
                query,
                params
            )

            row = cur.fetchone()

    if not row:

        raise Exception(
            f"""
No PACKING task found.

ORDER:
{order_id}

STATUS:
{status}
"""
        )

    return row[0]


# =====================================================
# TABLE COUNTS
# =====================================================

TABLES = [

    "purchase_orders",

    "purchase_order_items",

    "shipments",

    "shipment_items",

    "shipment_transportation",

    "shipment_tracking",

    "warehouse_tasks",

    "inventory",

    "inventory_transactions",

    "orders",

    "order_items",

    "inventory_allocations",

    "inventory_reservations",

    "packages"
]


def table_counts():

    with get_db() as conn:

        with conn.cursor() as cur:

            result = {}

            for table in TABLES:

                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    """
                )

                result[table] = cur.fetchone()[0]

    return result


# =====================================================
# EXECUTE EVENT
# =====================================================

def execute_event(
    event,
    file,
    args=None
):

    print("\n")
    print("=" * 70)
    print(event)
    print("=" * 70)

    before = table_counts()

    script_path = os.path.join(
        PROJECT_ROOT,
        file.replace("/", os.sep)
    )

    command = [
        PYTHON,
        script_path
    ]

    if args:

        command.extend(
            args
        )

    start = time.time()

    process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )

    duration = round(
        time.time() - start,
        2
    )

    after = table_counts()

    status = (
        "SUCCESS"
        if process.returncode == 0
        else "FAILED"
    )

    changes = {}

    for table in before:

        difference = (
            after[table]
            -
            before[table]
        )

        if difference:

            changes[table] = difference

    result = {

        "event": event,

        "file": file,

        "status": status,

        "duration_seconds": duration,

        "stdout": process.stdout,

        "stderr": process.stderr,

        "table_changes": changes,

        "executed_at":
            datetime.now().isoformat()
    }

    print(
        "STATUS:",
        status
    )

    if process.stdout:

        print(
            process.stdout
        )

    if process.stderr:

        print(
            process.stderr
        )

    return result


# =====================================================
# MAIN FLOW
# =====================================================

def main():

    report = []

    context = {}

    for event, file in FLOW:

        args = None

        # =================================================
        # ORDER CREATED
        # =================================================

        if event == "OrderCreated":

            # Script itself creates the order.
            # We fetch it immediately after success.

            args = None


        # =================================================
        # ORDER ITEM CREATED
        # =================================================

        elif event == "OrderItemCreated":

            order_id = context.get(
                "order_id"
            )

            if not order_id:

                order_id = get_latest_order_id()

                context["order_id"] = order_id

            print(
                "Using Order:",
                order_id
            )

            args = [
                order_id
            ]


        # =================================================
        # INVENTORY ALLOCATION CREATED
        # =================================================

        elif event == "InventoryAllocationCreated":

            order_id = context.get(
                "order_id"
            )

            if not order_id:

                order_id = get_latest_order_id()

                context["order_id"] = order_id

            print(
                "Using Order:",
                order_id
            )

            args = [
                order_id
            ]


        # =================================================
        # INVENTORY RESERVED
        # =================================================

        elif event == "InventoryReserved":

            order_id = context.get(
                "order_id"
            )

            if not order_id:

                raise Exception(
                    "No current order in flow context"
                )

            print(
                "Using Order:",
                order_id
            )

            args = [
                order_id
            ]


        # =================================================
        # PICKING TASK CREATED
        # =================================================

        elif event == "PickingTaskCreated":

            order_id = context.get(
                "order_id"
            )

            if not order_id:

                raise Exception(
                    "No current order for PickingTaskCreated"
                )

            allocation_id = get_latest_allocation_id(
                order_id
            )

            print(
                "Using Allocation:",
                allocation_id
            )

            context["allocation_id"] = allocation_id

            args = [
                allocation_id
            ]


        # =================================================
        # PICKING TASK STARTED
        # =================================================

        elif event == "PickingTaskStarted":

            picking_task_id = context.get(
                "picking_task_id"
            )

            if not picking_task_id:

                allocation_id = context.get(
                    "allocation_id"
                )

                if not allocation_id:

                    raise Exception(
                        "No allocation in flow context"
                    )

                picking_task_id = (
                    get_picking_task_for_allocation(
                        allocation_id,
                        "CREATED"
                    )
                )

                context["picking_task_id"] = (
                    picking_task_id
                )

            print(
                "Starting Picking Task:",
                picking_task_id
            )

            args = [
                picking_task_id
            ]


        # =================================================
        # PICKING COMPLETED
        # =================================================

        elif event == "PickingCompleted":

            picking_task_id = context.get(
                "picking_task_id"
            )

            if not picking_task_id:

                allocation_id = context.get(
                    "allocation_id"
                )

                if not allocation_id:

                    raise Exception(
                        "No allocation in flow context"
                    )

                picking_task_id = (
                    get_picking_task_for_allocation(
                        allocation_id,
                        "STARTED"
                    )
                )

                context["picking_task_id"] = (
                    picking_task_id
                )

            print(
                "Completing Picking Task:",
                picking_task_id
            )

            args = [
                picking_task_id
            ]


        # =================================================
        # PACKING TASK CREATED
        # =================================================

        elif event == "PackingTaskCreated":

            picking_task_id = context.get(
                "picking_task_id"
            )

            if not picking_task_id:

                allocation_id = context.get(
                    "allocation_id"
                )

                if not allocation_id:

                    raise Exception(
                        "No allocation in flow context"
                    )

                picking_task_id = (
                    get_picking_task_for_allocation(
                        allocation_id,
                        "COMPLETED"
                    )
                )

                context["picking_task_id"] = (
                    picking_task_id
                )

            print(
                "Using Completed Picking Task:",
                picking_task_id
            )

            args = [
                picking_task_id
            ]


        # =================================================
        # PACKING TASK STARTED
        # =================================================

        elif event == "PackingTaskStarted":

            packing_task_id = context.get(
                "packing_task_id"
            )

            if not packing_task_id:

                order_id = context.get(
                    "order_id"
                )

                packing_task_id = (
                    get_latest_packing_task_id(
                        order_id,
                        "CREATED"
                    )
                )

                context["packing_task_id"] = (
                    packing_task_id
                )

            print(
                "Starting Packing Task:",
                packing_task_id
            )

            args = [
                packing_task_id
            ]


        # =================================================
        # PACKING COMPLETED
        # =================================================

        elif event == "PackingCompleted":

            packing_task_id = context.get(
                "packing_task_id"
            )

            if not packing_task_id:

                order_id = context.get(
                    "order_id"
                )

                packing_task_id = (
                    get_latest_packing_task_id(
                        order_id,
                        "STARTED"
                    )
                )

                context["packing_task_id"] = (
                    packing_task_id
                )

            print(
                "Completing Packing Task:",
                packing_task_id
            )

            args = [
                packing_task_id
            ]


        # =================================================
        # EXECUTE
        # =================================================

        result = execute_event(
            event,
            file,
            args
        )

        report.append(
            result
        )


        # =================================================
        # SAVE NEW IDS
        # =================================================

        if result["status"] == "SUCCESS":

            # ---------------------------------------------
            # Save order
            # ---------------------------------------------

            if event == "OrderCreated":

                order_id = get_latest_order_id()

                context["order_id"] = order_id

                print(
                    "Saved Order:",
                    order_id
                )


            # ---------------------------------------------
            # Save allocation
            # ---------------------------------------------

            elif event == "InventoryAllocationCreated":

                order_id = context["order_id"]

                allocation_id = get_latest_allocation_id(
                    order_id
                )

                context["allocation_id"] = (
                    allocation_id
                )

                print(
                    "Saved Allocation:",
                    allocation_id
                )


            # ---------------------------------------------
            # Save picking task
            # ---------------------------------------------

            elif event == "PickingTaskCreated":

                allocation_id = context["allocation_id"]

                picking_task_id = (
                    get_picking_task_for_allocation(
                        allocation_id
                    )
                )

                context["picking_task_id"] = (
                    picking_task_id
                )

                print(
                    "Saved Picking Task:",
                    picking_task_id
                )


            # ---------------------------------------------
            # Save packing task
            # ---------------------------------------------

            elif event == "PackingTaskCreated":

                order_id = context.get(
                    "order_id"
                )

                packing_task_id = (
                    get_latest_packing_task_id(
                        order_id
                    )
                )

                context["packing_task_id"] = (
                    packing_task_id
                )

                print(
                    "Saved Packing Task:",
                    packing_task_id
                )


        # =================================================
        # STOP ON FAILURE
        # =================================================

        if result["status"] == "FAILED":

            print(
                "\nFLOW STOPPED AT:",
                event
            )

            break


    # =====================================================
    # WRITE REPORT
    # =====================================================

    os.makedirs(
        os.path.dirname(OUTPUT_FILE),
        exist_ok=True
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file_handle:

        json.dump(
            report,
            file_handle,
            indent=4
        )

    print(
        "\nREPORT GENERATED:"
    )

    print(
        OUTPUT_FILE
    )


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":

    main()