import os
import json
import subprocess
import time

from datetime import datetime

import psycopg2
import psycopg2.extras


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
# DATABASE CONFIG
# =====================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 55432,
    "database": "supply_chain",
    "user": "supplychain_app",
    "password": "Cr7@1034"
}


# =====================================================
# STATIC EVENT FLOW
#
# Picking and packing task events are intentionally
# NOT placed here because they must be processed
# one task at a time.
# =====================================================

INBOUND_FLOW = [

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
    )
]


OUTBOUND_INITIAL_FLOW = [

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
    )
]


TRANSPORTATION_FLOW = [

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
# GENERATOR PATHS FOR DYNAMIC EVENTS
# =====================================================

PICKING_STARTED_FILE = (
    "generators/warehouse/task_started.py"
)

PICKING_COMPLETED_FILE = (
    "generators/warehouse/picking_completed.py"
)

PACKING_CREATED_FILE = (
    "generators/warehouse/packing_task_created.py"
)

PACKING_STARTED_FILE = (
    "generators/warehouse/task_started.py"
)

PACKING_COMPLETED_FILE = (
    "generators/warehouse/packing_completed.py"
)


# =====================================================
# DATABASE CONNECTION
# =====================================================

def get_db():

    return psycopg2.connect(
        **DB_CONFIG
    )


# =====================================================
# LATEST ORDER
# =====================================================

def get_latest_order_id():

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    order_id
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
# ALLOCATIONS FOR EXACT ORDER
# =====================================================

def get_latest_allocation_ids(order_id):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    allocation_id
                FROM inventory_allocations
                WHERE order_id=%s
                ORDER BY allocated_at ASC
                """,
                (
                    order_id,
                )
            )

            rows = cur.fetchall()

    if not rows:

        raise Exception(
            f"No allocations found for {order_id}"
        )

    return [
        row[0]
        for row in rows
    ]


# =====================================================
# PICKING TASKS FOR EXACT ORDER
# =====================================================

def get_picking_task_ids(order_id):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    task_id
                FROM warehouse_tasks
                WHERE task_type='PICKING'
                  AND order_id=%s
                ORDER BY created_at ASC
                """,
                (
                    order_id,
                )
            )

            rows = cur.fetchall()

    if not rows:

        raise Exception(
            f"No picking tasks found for {order_id}"
        )

    return [
        row[0]
        for row in rows
    ]


# =====================================================
# PACKING TASKS FOR EXACT ORDER
# =====================================================

def get_packing_task_ids(order_id):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    task_id
                FROM warehouse_tasks
                WHERE task_type='PACKING'
                  AND order_id=%s
                ORDER BY created_at ASC
                """,
                (
                    order_id,
                )
            )

            rows = cur.fetchall()

    return [
        row[0]
        for row in rows
    ]


# =====================================================
# PICKING TASK STATUS
# =====================================================

def get_task_status(task_id):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    status
                FROM warehouse_tasks
                WHERE task_id=%s
                LIMIT 1
                """,
                (
                    task_id,
                )
            )

            row = cur.fetchone()

    if not row:

        raise Exception(
            f"Task not found: {task_id}"
        )

    return row[0]


# =====================================================
# PACKING TASKS CREATED FOR EXACT PICKING TASK
# =====================================================

def get_packing_task_for_picking_task(
    picking_task_id
):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    task_id
                FROM warehouse_tasks
                WHERE task_type='PACKING'
                  AND picking_task_id=%s
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (
                    picking_task_id,
                )
            )

            row = cur.fetchone()

    if not row:

        return None

    return row[0]


# =====================================================
# CHECK ORDER PICKING COMPLETION
# =====================================================

def all_picking_tasks_completed(order_id):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT COUNT(*)
                FROM warehouse_tasks
                WHERE task_type='PICKING'
                  AND order_id=%s
                  AND status <> 'COMPLETED'
                """,
                (
                    order_id,
                )
            )

            remaining = cur.fetchone()[0]

    return remaining == 0

# =====================================================
# TABLE SNAPSHOT CONFIGURATION
# =====================================================

TABLE_CONFIG = {

    "purchase_orders": {
        "order_by": "created_at",
        "limit": 20
    },

    "purchase_order_items": {
        "order_by": "po_id",
        "limit": 50
    },

    "shipments": {
        "order_by": "created_at",
        "limit": 20
    },

    "shipment_items": {
        "order_by": "shipment_id",
        "limit": 50
    },

    "shipment_transportation": {
        "order_by": "created_at",
        "limit": 20
    },

    "shipment_tracking": {
        "order_by": "created_at",
        "limit": 20
    },

    "warehouse_tasks": {
        "order_by": "created_at",
        "limit": 100
    },

    "inventory": {
        "order_by": "inventory_id",
        "limit": 100
    },

    "inventory_transactions": {
        "order_by": "created_at",
        "limit": 100
    },

    "orders": {
        "order_by": "created_at",
        "limit": 50
    },

    "order_items": {
        "order_by": "order_id",
        "limit": 100
    },

    "inventory_allocations": {
        "order_by": "allocated_at",
        "limit": 100
    },

    "inventory_reservations": {
        "order_by": "created_at",
        "limit": 100
    },

    "packages": {
        "order_by": "packed_at",
        "limit": 100
    }
}


# =====================================================
# JSON SERIALIZER
# =====================================================

def json_serializer(obj):

    if hasattr(obj, "isoformat"):
        return obj.isoformat()

    if isinstance(
        obj,
        (
            int,
            float,
            str,
            bool
        )
    ):
        return obj

    return str(obj)


# =====================================================
# TABLE COUNTS
# =====================================================

def table_counts():

    result = {}

    with get_db() as conn:

        with conn.cursor() as cur:

            for table in TABLE_CONFIG:

                try:

                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {table}
                        """
                    )

                    result[table] = cur.fetchone()[0]

                except Exception as exc:

                    result[table] = {
                        "error": str(exc)
                    }

    return result


# =====================================================
# FETCH DATABASE SNAPSHOT
# =====================================================

def fetch_table_data():

    snapshot = {}

    with get_db() as conn:

        with conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:

            for table, config in TABLE_CONFIG.items():

                try:

                    cur.execute(
                        f"""
                        SELECT *
                        FROM {table}
                        ORDER BY {config["order_by"]} DESC
                        LIMIT {config["limit"]}
                        """
                    )

                    rows = cur.fetchall()

                    snapshot[table] = [
                        dict(row)
                        for row in rows
                    ]

                except Exception as exc:

                    snapshot[table] = {
                        "error": str(exc)
                    }

    return snapshot


# =====================================================
# EXECUTE ONE EVENT
# =====================================================

def execute_event(
    event,
    file,
    args=None
):

    print()
    print("=" * 70)
    print(event)
    print("=" * 70)

    before = table_counts()

    script_path = os.path.join(
        PROJECT_ROOT,
        file.replace(
            "/",
            os.sep
        )
    )

    if not os.path.isfile(script_path):

        raise Exception(
            f"Generator file not found: {script_path}"
        )

    command = [
        PYTHON,
        script_path
    ]

    if args:

        command.extend(args)

    print(
        "COMMAND:",
        " ".join(
            f'"{x}"' if " " in x else x
            for x in command
        )
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

        before_value = before[table]
        after_value = after[table]

        if (
            isinstance(before_value, int)
            and isinstance(after_value, int)
        ):

            difference = (
                after_value - before_value
            )

            if difference != 0:

                changes[table] = difference

    result = {

        "event": event,

        "file": file,

        "arguments": args or [],

        "status": status,

        "return_code": process.returncode,

        "duration_seconds": duration,

        "stdout": process.stdout,

        "stderr": process.stderr,

        "table_changes": changes,

        "executed_at":
            datetime.now().isoformat()

    }

    if status == "SUCCESS":

        result["database_snapshot"] = (
            fetch_table_data()
        )

    else:

        result["database_snapshot"] = {}

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
# BUILD STATIC EVENT ARGUMENTS
# =====================================================

def build_static_event_args(
    event,
    context
):

    args = None

    # =================================================
    # ORDER ITEM CREATED
    # =================================================

    if event == "OrderItemCreated":

        order_id = context.get(
            "order_id"
        )

        if not order_id:

            raise Exception(
                "Order ID missing for OrderItemCreated"
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

            raise Exception(
                "Order ID missing for InventoryAllocationCreated"
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
                "Order ID missing for InventoryReserved"
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
                "Order ID missing for PickingTaskCreated"
            )

        args = [
            order_id
        ]


    # =================================================
    # PICKING TASK STARTED
    # =================================================

    elif event == "PickingTaskStarted":

        task_id = context.get(
            "current_picking_task"
        )

        if not task_id:

            raise Exception(
                "No current picking task available"
            )

        args = [
            task_id
        ]


    # =================================================
    # PICKING COMPLETED
    # =================================================

    elif event == "PickingCompleted":

        task_id = context.get(
            "current_picking_task"
        )

        if not task_id:

            raise Exception(
                "No current picking task available"
            )

        args = [
            task_id
        ]


    # =================================================
    # PACKING TASK CREATED
    # =================================================

    elif event == "PackingTaskCreated":

        # Your current packing_task_created.py
        # creates ONE task from one completed
        # picking task.
        #
        # It does not accept an argument.
        #
        args = None


    # =================================================
    # PACKING TASK STARTED
    # =================================================

    elif event == "PackingTaskStarted":

        task_id = context.get(
            "current_packing_task"
        )

        if not task_id:

            raise Exception(
                "No current packing task available"
            )

        args = [
            task_id
        ]


    # =================================================
    # PACKING COMPLETED
    # =================================================

    elif event == "PackingCompleted":

        # Current packing_completed.py does not
        # accept task_id. It searches for the
        # oldest STARTED packing task itself.
        #
        # Therefore no argument is passed.

        args = None


    # =================================================
    # DEFAULT
    # =================================================

    else:

        args = None

    return args


# =====================================================
# CHECK CURRENT PICKING TASK
# =====================================================

def refresh_current_picking_task(context):

    task_ids = context.get(
        "picking_task_ids",
        []
    )

    index = context.get(
        "picking_index",
        0
    )

    if index >= len(task_ids):

        context["current_picking_task"] = None

        return None

    current_task = task_ids[index]

    context["current_picking_task"] = (
        current_task
    )

    return current_task


# =====================================================
# CHECK CURRENT PACKING TASK
# =====================================================

def refresh_current_packing_task(context):

    task_ids = context.get(
        "packing_task_ids",
        []
    )

    index = context.get(
        "packing_index",
        0
    )

    if index >= len(task_ids):

        context["current_packing_task"] = None

        return None

    current_task = task_ids[index]

    context["current_packing_task"] = (
        current_task
    )

    return current_task

# =====================================================
# UPDATE CONTEXT AFTER STATIC EVENT
# =====================================================

def update_context_after_static_event(
    event,
    context
):

    # =================================================
    # ORDER CREATED
    # =================================================

    if event == "OrderCreated":

        order_id = get_latest_order_id()

        context["order_id"] = order_id

        print(
            "Saved Order:",
            order_id
        )

        return


    # =================================================
    # ALLOCATION CREATED
    # =================================================

    if event == "InventoryAllocationCreated":

        allocation_ids = get_latest_allocation_ids(
            context["order_id"]
        )

        context["allocation_ids"] = allocation_ids

        print(
            "Saved Allocations:",
            allocation_ids
        )

        return


    # =================================================
    # PICKING TASK CREATED
    # =================================================

    if event == "PickingTaskCreated":

        picking_task_ids = get_picking_task_ids(
            context["order_id"]
        )

        context["picking_task_ids"] = picking_task_ids
        context["picking_index"] = 0

        context["current_picking_task"] = (
            picking_task_ids[0]
            if picking_task_ids
            else None
        )

        print(
            "Saved Picking Tasks:",
            picking_task_ids
        )

        return


# =====================================================
# EXECUTE PICKING TASKS
# =====================================================

def execute_picking_tasks(
    context,
    report
):

    task_ids = list(
        context.get(
            "picking_task_ids",
            []
        )
    )

    if not task_ids:

        raise Exception(
            f"No picking tasks found for order "
            f"{context.get('order_id')}"
        )

    print()
    print("=" * 70)
    print("STARTING DYNAMIC PICKING FLOW")
    print("=" * 70)

    for index, task_id in enumerate(task_ids):

        context["picking_index"] = index
        context["current_picking_task"] = task_id

        # =================================================
        # VERIFY TASK EXISTS
        # =================================================

        current_status = get_task_status(
            task_id
        )

        print()
        print(
            f"Picking Task {index + 1}/{len(task_ids)}:"
            f" {task_id}"
        )

        print(
            "Current Status:",
            current_status
        )

        # =================================================
        # PICKING TASK STARTED
        # =================================================

        if current_status == "CREATED":

            result = execute_event(
                "PickingTaskStarted",
                PICKING_STARTED_FILE,
                [
                    task_id
                ]
            )

            report.append(result)

            if result["status"] != "SUCCESS":

                return False

        elif current_status == "STARTED":

            print(
                "Picking task already STARTED:",
                task_id
            )

        elif current_status == "COMPLETED":

            print(
                "Picking task already COMPLETED:",
                task_id
            )

            continue

        else:

            raise Exception(
                f"Picking task {task_id} "
                f"has unsupported status: "
                f"{current_status}"
            )

        # =================================================
        # PICKING COMPLETED
        # =================================================

        current_status = get_task_status(
            task_id
        )

        if current_status != "COMPLETED":

            result = execute_event(
                "PickingCompleted",
                PICKING_COMPLETED_FILE,
                [
                    task_id
                ]
            )

            report.append(result)

            if result["status"] != "SUCCESS":

                return False

        # =================================================
        # VERIFY COMPLETION
        # =================================================

        final_status = get_task_status(
            task_id
        )

        if final_status != "COMPLETED":

            raise Exception(
                f"Picking task did not complete: "
                f"{task_id}. "
                f"Current status: {final_status}"
            )

        print(
            "Picking Task Completed:",
            task_id
        )

    # =================================================
    # ALL PICKING COMPLETE
    # =================================================

    context["current_picking_task"] = None

    context["picking_index"] = len(
        task_ids
    )

    print()
    print(
        "ALL PICKING TASKS COMPLETED"
    )

    return True


# =====================================================
# EXECUTE PACKING TASK CREATION
# =====================================================

def execute_packing_task_creation(
    context,
    report
):

    created_packing_ids = []

    picking_task_ids = context.get(
        "picking_task_ids",
        []
    )

    if not picking_task_ids:

        raise Exception(
            "No picking tasks available "
            "for packing"
        )

    print()
    print("=" * 70)
    print("CREATING PACKING TASKS")
    print("=" * 70)

    # =================================================
    # CREATE ONE PACKING TASK FOR EACH
    # COMPLETED PICKING TASK
    # =================================================

    for picking_task_id in picking_task_ids:

        existing_packing_task = (
            get_packing_task_for_picking_task(
                picking_task_id
            )
        )

        if existing_packing_task:

            print(
                "Packing task already exists:",
                existing_packing_task,
                "for picking task:",
                picking_task_id
            )

            created_packing_ids.append(
                existing_packing_task
            )

            continue

        result = execute_event(
            "PackingTaskCreated",
            PACKING_CREATED_FILE,
            None
        )

        report.append(result)

        if result["status"] != "SUCCESS":

            return False

        # -------------------------------------------------
        # Find the packing task created for this exact
        # picking task.
        # -------------------------------------------------

        packing_task_id = (
            get_packing_task_for_picking_task(
                picking_task_id
            )
        )

        if not packing_task_id:

            raise Exception(
                "Packing task was reported as SUCCESS "
                f"but no packing task was created for "
                f"picking task {picking_task_id}"
            )

        created_packing_ids.append(
            packing_task_id
        )

        print(
            "Saved Packing Task:",
            packing_task_id
        )

    # =================================================
    # SAVE CONTEXT
    # =================================================

    context["packing_task_ids"] = (
        created_packing_ids
    )

    context["packing_index"] = 0

    context["current_packing_task"] = (
        created_packing_ids[0]
        if created_packing_ids
        else None
    )

    print(
        "All Packing Tasks:",
        created_packing_ids
    )

    return True


# =====================================================
# EXECUTE PACKING TASKS
# =====================================================

def execute_packing_tasks(
    context,
    report
):

    task_ids = list(
        context.get(
            "packing_task_ids",
            []
        )
    )

    if not task_ids:

        raise Exception(
            "No packing tasks found"
        )

    print()
    print("=" * 70)
    print("STARTING DYNAMIC PACKING FLOW")
    print("=" * 70)

    for index, task_id in enumerate(task_ids):

        context["packing_index"] = index
        context["current_packing_task"] = task_id

        print()
        print(
            f"Packing Task {index + 1}/"
            f"{len(task_ids)}: {task_id}"
        )

        current_status = get_task_status(
            task_id
        )

        print(
            "Current Status:",
            current_status
        )

        # =================================================
        # PACKING TASK STARTED
        # =================================================

        if current_status == "CREATED":

            result = execute_event(
                "PackingTaskStarted",
                PACKING_STARTED_FILE,
                [
                    task_id
                ]
            )

            report.append(result)

            if result["status"] != "SUCCESS":

                return False

        elif current_status == "STARTED":

            print(
                "Packing task already STARTED:",
                task_id
            )

        elif current_status == "COMPLETED":

            print(
                "Packing task already COMPLETED:",
                task_id
            )

            continue

        else:

            raise Exception(
                f"Packing task {task_id} "
                f"has unsupported status: "
                f"{current_status}"
            )

        # =================================================
        # PACKING COMPLETED
        #
        # IMPORTANT:
        # Current packing_completed.py does not accept
        # task_id. It searches for the oldest STARTED
        # PACKING task.
        #
        # Since we start exactly one packing task at a
        # time, this safely completes the current task.
        # =================================================

        current_status = get_task_status(
            task_id
        )

        if current_status != "COMPLETED":

            result = execute_event(
                "PackingCompleted",
                PACKING_COMPLETED_FILE,
                None
            )

            report.append(result)

            if result["status"] != "SUCCESS":

                return False

        # =================================================
        # VERIFY COMPLETION
        # =================================================

        final_status = get_task_status(
            task_id
        )

        if final_status != "COMPLETED":

            raise Exception(
                f"Packing task did not complete: "
                f"{task_id}. "
                f"Current status: {final_status}"
            )

        print(
            "Packing Task Completed:",
            task_id
        )

    # =================================================
    # ALL PACKING COMPLETE
    # =================================================

    context["current_packing_task"] = None

    context["packing_index"] = len(
        task_ids
    )

    print()
    print(
        "ALL PACKING TASKS COMPLETED"
    )

    return True


# =====================================================
# EXECUTE STATIC EVENT
# =====================================================

def execute_static_flow_event(
    event,
    file,
    context,
    report
):

    args = build_static_event_args(
        event,
        context
    )

    result = execute_event(
        event,
        file,
        args
    )

    report.append(result)

    if result["status"] != "SUCCESS":

        print(
            "\nFLOW STOPPED AT:",
            event
        )

        return False

    update_context_after_static_event(
        event,
        context
    )

    return True


# =====================================================
# SAVE REPORT
# =====================================================

def save_report(report):

    os.makedirs(
        os.path.dirname(
            OUTPUT_FILE
        ),
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
            indent=4,
            default=json_serializer
        )

    print()
    print(
        "REPORT GENERATED:"
    )

    print(
        OUTPUT_FILE
    )


# =====================================================
# MAIN FLOW
# =====================================================

def main():

    report = []

    context = {

        "order_id": None,

        "allocation_ids": [],

        "picking_task_ids": [],

        "picking_index": 0,

        "current_picking_task": None,

        "packing_task_ids": [],

        "packing_index": 0,

        "current_packing_task": None
    }

    try:

        # =================================================
        # 1. INBOUND
        # =================================================

        for event, file in INBOUND_FLOW:

            success = execute_static_flow_event(
                event,
                file,
                context,
                report
            )

            if not success:

                return

        # =================================================
        # 2. INITIAL OUTBOUND FLOW
        # =================================================

        for event, file in OUTBOUND_INITIAL_FLOW:

            success = execute_static_flow_event(
                event,
                file,
                context,
                report
            )

            if not success:

                return

        # =================================================
        # 3. PICKING
        # =================================================

        picking_success = execute_picking_tasks(
            context,
            report
        )

        if not picking_success:

            print(
                "\nFLOW STOPPED DURING PICKING"
            )

            return

        # =================================================
        # 4. PACKING TASK CREATION
        # =================================================

        packing_creation_success = (
            execute_packing_task_creation(
                context,
                report
            )
        )

        if not packing_creation_success:

            print(
                "\nFLOW STOPPED AT PackingTaskCreated"
            )

            return

        # =================================================
        # 5. PACKING
        # =================================================

        packing_success = execute_packing_tasks(
            context,
            report
        )

        if not packing_success:

            print(
                "\nFLOW STOPPED DURING PACKING"
            )

            return

        # =================================================
        # 6. TRANSPORTATION
        # =================================================

        for event, file in TRANSPORTATION_FLOW:

            success = execute_static_flow_event(
                event,
                file,
                context,
                report
            )

            if not success:

                return

        # =================================================
        # 7. SUCCESS
        # =================================================

        print()
        print("=" * 70)
        print("COMPLETE EVENT FLOW FINISHED SUCCESSFULLY")
        print("=" * 70)

    except Exception as exc:

        print()
        print(
            "\nFLOW ERROR:"
        )

        print(
            exc
        )

        report.append(
            {
                "event": "FLOW",
                "status": "FAILED",
                "error": str(exc),
                "executed_at":
                    datetime.now().isoformat()
            }
        )

    finally:

        save_report(report)


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":

    main()