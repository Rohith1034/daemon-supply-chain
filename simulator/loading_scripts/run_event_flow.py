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


# =====================================================
# DYNAMIC EVENT GENERATORS
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
# TRANSPORTATION EVENTS
# =====================================================

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
# TRANSPORTATION STATE ORDER
# =====================================================

TRANSPORTATION_EVENT_INDEX = {
    "ShipmentReady": 0,
    "CarrierAssigned": 1,
    "ShipmentPickedUp": 2,
    "ShipmentInTransit": 3,
    "ShipmentDelivered": 4
}


TRANSPORTATION_STATUS_INDEX = {
    "READY": 0,
    "ASSIGNED": 1,
    "PICKED_UP": 2,
    "IN_TRANSIT": 3,
    "DELIVERED": 5
}


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
# ALLOCATIONS
# =====================================================

def get_latest_allocation_ids(
    order_id
):

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
# PICKING TASKS
# =====================================================

def get_picking_task_ids(
    order_id
):

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
# PACKING TASK FOR PICKING TASK
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
# TASK STATUS
# =====================================================

def get_task_status(
    task_id
):

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
# ALL PACKED PACKAGES FOR EXACT ORDER
# =====================================================

def get_packed_package_ids(
    order_id
):
    """
    Return every PACKED package belonging to the
    supplied order.

    A package may already have an outbound shipment.
    The transportation flow will inspect that shipment
    and resume from its current state.

    A DELIVERED shipment will be skipped completely.
    """

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    package_id
                FROM packages
                WHERE order_id=%s
                  AND package_status='PACKED'
                ORDER BY packed_at ASC,
                         package_id ASC
                """,
                (
                    order_id,
                )
            )

            rows = cur.fetchall()

    if not rows:

        raise Exception(
            f"No PACKED packages found "
            f"for order {order_id}"
        )

    return [
        row[0]
        for row in rows
    ]


# =====================================================
# OUTBOUND SHIPMENT DETAILS FOR EXACT PACKAGE
# =====================================================

def get_outbound_shipment_for_package(
    package_id
):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    shipment_id,
                    shipment_status,
                    fulfillment_id
                FROM outbound_shipments
                WHERE package_id=%s
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (
                    package_id,
                )
            )

            row = cur.fetchone()

    if not row:

        return None

    return {
        "shipment_id": row[0],
        "shipment_status": row[1],
        "fulfillment_id": row[2]
    }


# =====================================================
# OUTBOUND FULFILLMENT FOR EXACT SHIPMENT
# =====================================================

def get_outbound_fulfillment_id(
    shipment_id
):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    fulfillment_id
                FROM outbound_shipments
                WHERE shipment_id=%s
                LIMIT 1
                """,
                (
                    shipment_id,
                )
            )

            row = cur.fetchone()

    if not row:

        return None

    return row[0]


# =====================================================
# TRANSPORTATION DETAILS
# =====================================================

def get_outbound_transportation(
    shipment_id
):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    vehicle_id,
                    trailer_id,
                    driver_id
                FROM outbound_shipment_transportation
                WHERE shipment_id=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    shipment_id,
                )
            )

            row = cur.fetchone()

    if not row:

        return None

    return {
        "vehicle_id": row[0],
        "trailer_id": row[1],
        "driver_id": row[2]
    }


# =====================================================
# TRACKING DETAILS
# =====================================================

def get_outbound_tracking_id(
    shipment_id
):

    with get_db() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    tracking_id
                FROM outbound_shipment_tracking
                WHERE shipment_id=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    shipment_id,
                )
            )

            row = cur.fetchone()

    if not row:

        return None

    return row[0]


# =====================================================
# TABLE SNAPSHOT CONFIGURATION
# =====================================================

TABLE_CONFIG = {

    # ------------------------------
    # Inbound
    # ------------------------------

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
        "order_by": "assigned_at",
        "limit": 20
    },

    "shipment_loading_events": {
        "order_by": "loaded_at",
        "limit": 20
    },

    "shipment_tracking": {
        "order_by": "created_at",
        "limit": 20
    },

    # ------------------------------
    # Warehouse
    # ------------------------------

    "warehouse_tasks": {
        "order_by": "created_at",
        "limit": 100
    },

    # ------------------------------
    # Inventory
    # ------------------------------

    "inventory": {
        "order_by": "inventory_id",
        "limit": 100
    },

    "inventory_transactions": {
        "order_by": "created_at",
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

    # ------------------------------
    # Orders
    # ------------------------------

    "orders": {
        "order_by": "created_at",
        "limit": 50
    },

    "order_items": {
        "order_by": "order_id",
        "limit": 100
    },

    # ------------------------------
    # Packages
    # ------------------------------

    "packages": {
        "order_by": "packed_at",
        "limit": 100
    },

    "package_items": {
        "order_by": "package_id",
        "limit": 100
    },

    # ------------------------------
    # Outbound fulfillment
    # ------------------------------

    "outbound_fulfillment": {
        "order_by": "created_at",
        "limit": 50
    },

    "outbound_shipments": {
        "order_by": "created_at",
        "limit": 100
    },

    "outbound_shipment_transportation": {
        "order_by": "created_at",
        "limit": 100
    },

    "outbound_shipment_loading_events": {
        "order_by": "loaded_at",
        "limit": 100
    },

    "outbound_shipment_tracking": {
        "order_by": "created_at",
        "limit": 100
    }
}


# =====================================================
# JSON SERIALIZER
# =====================================================

def json_serializer(obj):

    if hasattr(
        obj,
        "isoformat"
    ):

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

                    result[table] = (
                        cur.fetchone()[0]
                    )

                except Exception as exc:

                    result[table] = {
                        "error": str(exc)
                    }

    return result


# =====================================================
# DATABASE SNAPSHOT
# =====================================================

def fetch_table_data():

    snapshot = {}

    with get_db() as conn:

        with conn.cursor(
            cursor_factory=(
                psycopg2.extras.RealDictCursor
            )
        ) as cur:

            for table, config in (
                TABLE_CONFIG.items()
            ):

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

    if not os.path.isfile(
        script_path
    ):

        raise Exception(
            f"Generator file not found: "
            f"{script_path}"
        )

    command = [
        PYTHON,
        script_path
    ]

    if args:

        command.extend(
            args
        )

    print(
        "COMMAND:",
        " ".join(
            f'"{x}"'
            if " " in x
            else x
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
            isinstance(
                before_value,
                int
            )
            and
            isinstance(
                after_value,
                int
            )
        ):

            difference = (
                after_value -
                before_value
            )

            if difference != 0:

                changes[table] = (
                    difference
                )

    result = {

        "event":
            event,

        "file":
            file,

        "arguments":
            args or [],

        "status":
            status,

        "return_code":
            process.returncode,

        "duration_seconds":
            duration,

        "stdout":
            process.stdout,

        "stderr":
            process.stderr,

        "table_changes":
            changes,

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
# BUILD EVENT ARGUMENTS
# =====================================================

def build_static_event_args(
    event,
    context
):

    args = None

    # =================================================
    # ORDER EVENTS
    # =================================================

    if event == "OrderItemCreated":

        args = [
            context["order_id"]
        ]

    elif event == "InventoryAllocationCreated":

        args = [
            context["order_id"]
        ]

    elif event == "InventoryReserved":

        args = [
            context["order_id"]
        ]

    elif event == "PickingTaskCreated":

        args = [
            context["order_id"]
        ]

    # =================================================
    # PICKING
    # =================================================

    elif event == "PickingTaskStarted":

        args = [
            context["current_picking_task"]
        ]

    elif event == "PickingCompleted":

        args = [
            context["current_picking_task"]
        ]

    # =================================================
    # PACKING
    # =================================================

    elif event == "PackingTaskCreated":

        args = [
            context["current_picking_task"]
        ]

    elif event == "PackingTaskStarted":

        args = [
            context["current_packing_task"]
        ]

    elif event == "PackingCompleted":

        args = [
            context["current_packing_task"]
        ]

    # =================================================
    # TRANSPORTATION
    # =================================================

    elif event == "ShipmentReady":

        args = [
            context["current_package_id"]
        ]

    elif event == "CarrierAssigned":

        args = [
            context["current_outbound_shipment_id"]
        ]

    elif event == "ShipmentPickedUp":

        args = [
            context["current_outbound_shipment_id"]
        ]

    elif event == "ShipmentInTransit":

        args = [
            context["current_outbound_shipment_id"]
        ]

    elif event == "ShipmentDelivered":

        args = [
            context["current_outbound_shipment_id"]
        ]

    return args


# =====================================================
# UPDATE CONTEXT AFTER EVENT
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

        context["order_id"] = (
            order_id
        )

        print(
            "Saved Order:",
            order_id
        )

        return


    # =================================================
    # ALLOCATION CREATED
    # =================================================

    if event == "InventoryAllocationCreated":

        allocation_ids = (
            get_latest_allocation_ids(
                context["order_id"]
            )
        )

        context["allocation_ids"] = (
            allocation_ids
        )

        print(
            "Saved Allocations:",
            allocation_ids
        )

        return


    # =================================================
    # PICKING TASK CREATED
    # =================================================

    if event == "PickingTaskCreated":

        picking_task_ids = (
            get_picking_task_ids(
                context["order_id"]
            )
        )

        context["picking_task_ids"] = (
            picking_task_ids
        )

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


    # =================================================
    # SHIPMENT READY
    # =================================================

    if event == "ShipmentReady":

        package_id = context[
            "current_package_id"
        ]

        shipment_details = (
            get_outbound_shipment_for_package(
                package_id
            )
        )

        if not shipment_details:

            raise Exception(
                f"""
ShipmentReady succeeded but no outbound
shipment was found.

PACKAGE:
{package_id}
"""
            )

        shipment_id = (
            shipment_details[
                "shipment_id"
            ]
        )

        context[
            "current_outbound_shipment_id"
        ] = shipment_id

        context[
            "current_outbound_fulfillment_id"
        ] = (
            shipment_details[
                "fulfillment_id"
            ]
        )

        print(
            "Saved Outbound Shipment:",
            shipment_id
        )

        print(
            "Saved Outbound Fulfillment:",
            context[
                "current_outbound_fulfillment_id"
            ]
        )

        return


    # =================================================
    # CARRIER ASSIGNED
    # =================================================

    if event == "CarrierAssigned":

        shipment_id = context[
            "current_outbound_shipment_id"
        ]

        transportation = (
            get_outbound_transportation(
                shipment_id
            )
        )

        if not transportation:

            raise Exception(
                f"""
CarrierAssigned succeeded but transportation
record was not found.

SHIPMENT:
{shipment_id}
"""
            )

        context[
            "current_vehicle_id"
        ] = (
            transportation[
                "vehicle_id"
            ]
        )

        context[
            "current_trailer_id"
        ] = (
            transportation[
                "trailer_id"
            ]
        )

        context[
            "current_driver_id"
        ] = (
            transportation[
                "driver_id"
            ]
        )

        print(
            "Saved Vehicle:",
            context["current_vehicle_id"]
        )

        print(
            "Saved Trailer:",
            context["current_trailer_id"]
        )

        print(
            "Saved Driver:",
            context["current_driver_id"]
        )

        return


    # =================================================
    # SHIPMENT PICKED UP
    # =================================================

    if event == "ShipmentPickedUp":

        shipment_id = context[
            "current_outbound_shipment_id"
        ]

        tracking_id = (
            get_outbound_tracking_id(
                shipment_id
            )
        )

        if not tracking_id:

            raise Exception(
                f"""
ShipmentPickedUp succeeded but no tracking
record was found.

SHIPMENT:
{shipment_id}
"""
            )

        context[
            "current_tracking_id"
        ] = tracking_id

        print(
            "Saved Tracking:",
            tracking_id
        )

        return


    # =================================================
    # SHIPMENT IN TRANSIT
    # =================================================

    if event == "ShipmentInTransit":

        shipment_id = context[
            "current_outbound_shipment_id"
        ]

        tracking_id = (
            get_outbound_tracking_id(
                shipment_id
            )
        )

        if not tracking_id:

            raise Exception(
                f"""
ShipmentInTransit completed but tracking
record could not be found.

SHIPMENT:
{shipment_id}
"""
            )

        context[
            "current_tracking_id"
        ] = tracking_id

        print(
            "Confirmed Tracking:",
            tracking_id
        )

        return


    # =================================================
    # SHIPMENT DELIVERED
    # =================================================

    if event == "ShipmentDelivered":

        shipment_id = context[
            "current_outbound_shipment_id"
        ]

        tracking_id = (
            get_outbound_tracking_id(
                shipment_id
            )
        )

        if not tracking_id:

            raise Exception(
                f"""
ShipmentDelivered completed but tracking
record could not be found.

SHIPMENT:
{shipment_id}
"""
            )

        context[
            "current_tracking_id"
        ] = tracking_id

        print(
            "Confirmed Delivered Tracking:",
            tracking_id
        )

        return


# =====================================================
# PICKING FLOW
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

    for index, task_id in enumerate(
        task_ids
    ):

        context[
            "picking_index"
        ] = index

        context[
            "current_picking_task"
        ] = task_id

        current_status = get_task_status(
            task_id
        )

        print()
        print(
            f"Picking Task {index + 1}/"
            f"{len(task_ids)}: "
            f"{task_id}"
        )

        print(
            "Current Status:",
            current_status
        )

        if current_status == "CREATED":

            result = execute_event(
                "PickingTaskStarted",
                PICKING_STARTED_FILE,
                [
                    task_id
                ]
            )

            report.append(
                result
            )

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
                f"Unsupported picking status "
                f"{current_status} for {task_id}"
            )

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

            report.append(
                result
            )

            if result["status"] != "SUCCESS":

                return False

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

    context[
        "current_picking_task"
    ] = None

    context[
        "picking_index"
    ] = len(task_ids)

    print()
    print(
        "ALL PICKING TASKS COMPLETED"
    )

    return True


# =====================================================
# PACKING TASK CREATION
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
            "No picking tasks available for packing"
        )

    print()
    print("=" * 70)
    print("CREATING PACKING TASKS")
    print("=" * 70)

    for picking_task_id in (
        picking_task_ids
    ):

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

        context[
            "current_picking_task"
        ] = picking_task_id

        result = execute_event(
            "PackingTaskCreated",
            PACKING_CREATED_FILE,
            [
                picking_task_id
            ]
        )

        report.append(
            result
        )

        if result["status"] != "SUCCESS":

            return False

        packing_task_id = (
            get_packing_task_for_picking_task(
                picking_task_id
            )
        )

        if not packing_task_id:

            raise Exception(
                f"""
PackingTaskCreated reported SUCCESS,
but no packing task exists for:

{picking_task_id}
"""
            )

        created_packing_ids.append(
            packing_task_id
        )

        print(
            "Saved Packing Task:",
            packing_task_id,
            "for Picking Task:",
            picking_task_id
        )

    context[
        "current_picking_task"
    ] = None

    context[
        "packing_task_ids"
    ] = created_packing_ids

    context[
        "packing_index"
    ] = 0

    context[
        "current_packing_task"
    ] = (
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
# PACKING FLOW
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

    for index, task_id in enumerate(
        task_ids
    ):

        context[
            "packing_index"
        ] = index

        context[
            "current_packing_task"
        ] = task_id

        print()
        print(
            f"Packing Task {index + 1}/"
            f"{len(task_ids)}: "
            f"{task_id}"
        )

        current_status = get_task_status(
            task_id
        )

        print(
            "Current Status:",
            current_status
        )

        if current_status == "CREATED":

            result = execute_event(
                "PackingTaskStarted",
                PACKING_STARTED_FILE,
                [
                    task_id
                ]
            )

            report.append(
                result
            )

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
                f"Unsupported packing status "
                f"{current_status} for {task_id}"
            )

        current_status = get_task_status(
            task_id
        )

        if current_status != "COMPLETED":

            result = execute_event(
                "PackingCompleted",
                PACKING_COMPLETED_FILE,
                [
                    task_id
                ]
            )

            report.append(
                result
            )

            if result["status"] != "SUCCESS":

                return False

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

    context[
        "current_packing_task"
    ] = None

    context[
        "packing_index"
    ] = len(task_ids)

    print()
    print(
        "ALL PACKING TASKS COMPLETED"
    )

    return True


# =====================================================
# TRANSPORTATION FOR ALL PACKAGES
# =====================================================

def execute_transportation_for_packages(
    context,
    report
):

    order_id = context.get(
        "order_id"
    )

    if not order_id:

        raise Exception(
            "Order ID missing before transportation"
        )

    package_ids = get_packed_package_ids(
        order_id
    )

    context[
        "package_ids"
    ] = package_ids

    print()
    print("=" * 70)
    print(
        "STARTING TRANSPORTATION FLOW FOR "
        f"{len(package_ids)} PACKAGE(S)"
    )
    print("=" * 70)

    for index, package_id in enumerate(
        package_ids
    ):

        context[
            "package_index"
        ] = index

        context[
            "current_package_id"
        ] = package_id

        context[
            "current_outbound_shipment_id"
        ] = None

        context[
            "current_outbound_fulfillment_id"
        ] = None

        context[
            "current_vehicle_id"
        ] = None

        context[
            "current_trailer_id"
        ] = None

        context[
            "current_driver_id"
        ] = None

        context[
            "current_tracking_id"
        ] = None

        print()
        print("=" * 70)
        print(
            f"PACKAGE {index + 1}/"
            f"{len(package_ids)}: "
            f"{package_id}"
        )
        print("=" * 70)

        # =================================================
        # FIND EXISTING SHIPMENT
        # =================================================

        shipment_details = (
            get_outbound_shipment_for_package(
                package_id
            )
        )

        if shipment_details:

            shipment_id = (
                shipment_details[
                    "shipment_id"
                ]
            )

            shipment_status = (
                shipment_details[
                    "shipment_status"
                ]
            )

            fulfillment_id = (
                shipment_details[
                    "fulfillment_id"
                ]
            )

            context[
                "current_outbound_shipment_id"
            ] = shipment_id

            context[
                "current_outbound_fulfillment_id"
            ] = fulfillment_id

            print(
                "Existing outbound shipment found:",
                shipment_id
            )

            print(
                "Current shipment status:",
                shipment_status
            )

            # =================================================
            # ALREADY DELIVERED
            #
            # Nothing else should happen for this package.
            # =================================================

            if shipment_status == "DELIVERED":

                print()
                print(
                    "Shipment already DELIVERED for package:",
                    package_id
                )

                print(
                    "Skipping transportation flow."
                )

                tracking_id = (
                    get_outbound_tracking_id(
                        shipment_id
                    )
                )

                if tracking_id:

                    context[
                        "current_tracking_id"
                    ] = tracking_id

                transportation = (
                    get_outbound_transportation(
                        shipment_id
                    )
                )

                if transportation:

                    context[
                        "current_vehicle_id"
                    ] = (
                        transportation[
                            "vehicle_id"
                        ]
                    )

                    context[
                        "current_trailer_id"
                    ] = (
                        transportation[
                            "trailer_id"
                        ]
                    )

                    context[
                        "current_driver_id"
                    ] = (
                        transportation[
                            "driver_id"
                        ]
                    )

                print(
                    "TRANSPORTATION ALREADY COMPLETED FOR PACKAGE:",
                    package_id
                )

                continue

        # =================================================
        # EXECUTE TRANSPORTATION EVENTS
        #
        # If a shipment already exists, resume from its
        # current state instead of restarting at READY.
        # =================================================

        for event, file in (
            TRANSPORTATION_FLOW
        ):

            # ---------------------------------------------
            # No shipment exists yet.
            #
            # ShipmentReady is required first.
            # ---------------------------------------------

            if not shipment_details:

                if event != "ShipmentReady":

                    raise Exception(
                        f"""
No outbound shipment exists for package.

PACKAGE:
{package_id}

EVENT:
{event}
"""
                    )

                result = (
                    execute_static_flow_event(
                        event,
                        file,
                        context,
                        report
                    )
                )

                if not result:

                    return False

                # Refresh shipment state after creation.
                shipment_details = (
                    get_outbound_shipment_for_package(
                        package_id
                    )
                )

                if not shipment_details:

                    raise Exception(
                        f"""
ShipmentReady completed but shipment
could not be found afterward.

PACKAGE:
{package_id}
"""
                    )

                continue

            # ---------------------------------------------
            # Shipment already exists.
            #
            # Determine what event is required next.
            # ---------------------------------------------

            shipment_status = (
                shipment_details[
                    "shipment_status"
                ]
            )

            # ---------------------------------------------
            # READY
            # ---------------------------------------------

            if shipment_status == "READY":

                if event == "ShipmentReady":

                    print(
                        "ShipmentReady already completed "
                        "for package:",
                        package_id
                    )

                    continue

            # ---------------------------------------------
            # ASSIGNED
            # ---------------------------------------------

            elif shipment_status == "ASSIGNED":

                if event in (
                    "ShipmentReady",
                    "CarrierAssigned"
                ):

                    print(
                        f"{event} already completed "
                        f"for package {package_id}"
                    )

                    continue

            # ---------------------------------------------
            # PICKED UP
            # ---------------------------------------------

            elif shipment_status == "PICKED_UP":

                if event in (
                    "ShipmentReady",
                    "CarrierAssigned",
                    "ShipmentPickedUp"
                ):

                    print(
                        f"{event} already completed "
                        f"for package {package_id}"
                    )

                    continue

            # ---------------------------------------------
            # IN TRANSIT
            # ---------------------------------------------

            elif shipment_status == "IN_TRANSIT":

                if event in (
                    "ShipmentReady",
                    "CarrierAssigned",
                    "ShipmentPickedUp",
                    "ShipmentInTransit"
                ):

                    print(
                        f"{event} already completed "
                        f"for package {package_id}"
                    )

                    continue

            # ---------------------------------------------
            # DELIVERED
            #
            # This branch normally gets handled above,
            # but keep it here for safety.
            # ---------------------------------------------

            elif shipment_status == "DELIVERED":

                print(
                    "Shipment already DELIVERED for package:",
                    package_id
                )

                break

            # ---------------------------------------------
            # Unsupported status
            # ---------------------------------------------

            elif shipment_status not in (
                "READY",
                "ASSIGNED",
                "PICKED_UP",
                "IN_TRANSIT"
            ):

                raise Exception(
                    f"""
Unsupported outbound shipment status.

PACKAGE:
{package_id}

SHIPMENT:
{shipment_details["shipment_id"]}

STATUS:
{shipment_status}
"""
                )

            # ---------------------------------------------
            # Refresh context before later events.
            # ---------------------------------------------

            context[
                "current_outbound_shipment_id"
            ] = (
                shipment_details[
                    "shipment_id"
                ]
            )

            context[
                "current_outbound_fulfillment_id"
            ] = (
                shipment_details[
                    "fulfillment_id"
                ]
            )

            # ---------------------------------------------
            # Execute the required event.
            # ---------------------------------------------

            result = (
                execute_static_flow_event(
                    event,
                    file,
                    context,
                    report
                )
            )

            if not result:

                return False

            # ---------------------------------------------
            # Refresh shipment state after event.
            # ---------------------------------------------

            shipment_details = (
                get_outbound_shipment_for_package(
                    package_id
                )
            )

            if not shipment_details:

                raise Exception(
                    f"""
Outbound shipment disappeared after event.

PACKAGE:
{package_id}

EVENT:
{event}
"""
                )

            # ---------------------------------------------
            # If shipment has now reached DELIVERED,
            # stop processing further events for package.
            # ---------------------------------------------

            if (
                shipment_details[
                    "shipment_status"
                ]
                == "DELIVERED"
            ):

                break

        # =================================================
        # FINAL PACKAGE STATUS
        # =================================================

        final_shipment = (
            get_outbound_shipment_for_package(
                package_id
            )
        )

        if final_shipment:

            final_status = (
                final_shipment[
                    "shipment_status"
                ]
            )

            context[
                "current_outbound_shipment_id"
            ] = (
                final_shipment[
                    "shipment_id"
                ]
            )

            context[
                "current_outbound_fulfillment_id"
            ] = (
                final_shipment[
                    "fulfillment_id"
                ]
            )

            tracking_id = (
                get_outbound_tracking_id(
                    final_shipment[
                        "shipment_id"
                    ]
                )
            )

            context[
                "current_tracking_id"
            ] = tracking_id

            transportation = (
                get_outbound_transportation(
                    final_shipment[
                        "shipment_id"
                    ]
                )
            )

            if transportation:

                context[
                    "current_vehicle_id"
                ] = (
                    transportation[
                        "vehicle_id"
                    ]
                )

                context[
                    "current_trailer_id"
                ] = (
                    transportation[
                        "trailer_id"
                    ]
                )

                context[
                    "current_driver_id"
                ] = (
                    transportation[
                        "driver_id"
                    ]
                )

            if final_status != "DELIVERED":

                raise Exception(
                    f"""
Transportation flow did not finish.

PACKAGE:
{package_id}

SHIPMENT:
{final_shipment["shipment_id"]}

FINAL STATUS:
{final_status}
"""
                )

        print()
        print(
            "TRANSPORTATION COMPLETED FOR PACKAGE:",
            package_id
        )

        print(
            "SHIPMENT:",
            context[
                "current_outbound_shipment_id"
            ]
        )

        print(
            "TRACKING:",
            context[
                "current_tracking_id"
            ]
        )

    # ---------------------------------------------
    # All packages completed.
    # ---------------------------------------------

    context[
        "package_index"
    ] = len(package_ids)

    print()
    print(
        "=" * 70
    )
    print(
        "ALL PACKAGE TRANSPORTATION FLOWS COMPLETED"
    )
    print(
        "=" * 70
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

    if args is not None:

        for value in args:

            if not value:

                raise Exception(
                    f"""
Missing argument for event {event}.

Arguments:
{args}

Context:
{context}
"""
                )

    result = execute_event(
        event,
        file,
        args
    )

    report.append(
        result
    )

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
# MAIN FLOW
# =====================================================

def main():

    report = []

    context = {

        # ------------------------------
        # Order
        # ------------------------------

        "order_id":
            None,

        # ------------------------------
        # Inventory
        # ------------------------------

        "allocation_ids":
            [],

        # ------------------------------
        # Picking
        # ------------------------------

        "picking_task_ids":
            [],

        "picking_index":
            0,

        "current_picking_task":
            None,

        # ------------------------------
        # Packing
        # ------------------------------

        "packing_task_ids":
            [],

        "packing_index":
            0,

        "current_packing_task":
            None,

        # ------------------------------
        # Packages
        # ------------------------------

        "package_ids":
            [],

        "package_index":
            0,

        "current_package_id":
            None,

        # ------------------------------
        # Outbound
        # ------------------------------

        "current_outbound_fulfillment_id":
            None,

        "current_outbound_shipment_id":
            None,

        # ------------------------------
        # Transportation
        # ------------------------------

        "current_driver_id":
            None,

        "current_vehicle_id":
            None,

        "current_trailer_id":
            None,

        "current_tracking_id":
            None
    }

    try:

        # =================================================
        # 1. INBOUND
        # =================================================

        for event, file in (
            INBOUND_FLOW
        ):

            success = (
                execute_static_flow_event(
                    event,
                    file,
                    context,
                    report
                )
            )

            if not success:

                return

        # =================================================
        # 2. INITIAL OUTBOUND
        # =================================================

        for event, file in (
            OUTBOUND_INITIAL_FLOW
        ):

            success = (
                execute_static_flow_event(
                    event,
                    file,
                    context,
                    report
                )
            )

            if not success:

                return

        # =================================================
        # 3. PICKING
        # =================================================

        picking_success = (
            execute_picking_tasks(
                context,
                report
            )
        )

        if not picking_success:

            print(
                "\nFLOW STOPPED DURING PICKING"
            )

            return

        # =================================================
        # 4. CREATE PACKING TASKS
        # =================================================

        packing_creation_success = (
            execute_packing_task_creation(
                context,
                report
            )
        )

        if not packing_creation_success:

            print(
                "\nFLOW STOPPED AT "
                "PackingTaskCreated"
            )

            return

        # =================================================
        # 5. PACKING
        # =================================================

        packing_success = (
            execute_packing_tasks(
                context,
                report
            )
        )

        if not packing_success:

            print(
                "\nFLOW STOPPED DURING PACKING"
            )

            return

        # =================================================
        # 6. VERIFY ALL PACKAGES
        # =================================================

        package_ids = (
            get_packed_package_ids(
                context["order_id"]
            )
        )

        context[
            "package_ids"
        ] = package_ids

        print()
        print(
            "PACKED PACKAGES:",
            package_ids
        )

        print(
            "TOTAL PACKAGES:",
            len(package_ids)
        )

        # =================================================
        # 7. TRANSPORTATION
        # =================================================

        transportation_success = (
            execute_transportation_for_packages(
                context,
                report
            )
        )

        if not transportation_success:

            print(
                "\nFLOW STOPPED DURING "
                "TRANSPORTATION"
            )

            return

        # =================================================
        # 8. FINAL CLEANUP
        # =================================================

        context[
            "current_picking_task"
        ] = None

        context[
            "current_packing_task"
        ] = None

        context[
            "current_package_id"
        ] = None

        # =================================================
        # 9. SUCCESS
        # =================================================

        print()
        print("=" * 70)
        print(
            "COMPLETE EVENT FLOW "
            "FINISHED SUCCESSFULLY"
        )
        print("=" * 70)

        print()
        print(
            "FINAL CONTEXT:"
        )

        print(
            json.dumps(
                context,
                indent=4,
                default=json_serializer
            )
        )

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
                "event":
                    "FLOW",

                "status":
                    "FAILED",

                "error":
                    str(exc),

                "executed_at":
                    datetime.now().isoformat()
            }
        )

    finally:

        save_report(
            report
        )


# =====================================================
# SAVE REPORT
# =====================================================

def save_report(
    report
):

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
# ENTRY POINT
# =====================================================

if __name__ == "__main__":

    main()