import os
import subprocess
import sys
import uuid
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SIMULATOR_ROOT = ROOT / "simulator"
for path in (ROOT, SIMULATOR_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from core.db import Database
from core.simulation_clock import get_simulation_now
from loading_scripts import run_event_flow as flow


def _new_context():
    return {
        "correlation_id": os.getenv("SIMULATION_CORRELATION_ID") or str(uuid.uuid4()),
        "current_inventory_ids": [],
        "picking_task_ids": [],
        "packing_task_ids": [],
        "package_ids": [],
        "current_picking_task": None,
        "current_packing_task": None,
        "current_outbound_shipment_id": None,
    }


def _execute(event, relative_script, context, args=None):
    script = Path(flow.PROJECT_ROOT) / relative_script
    command = [sys.executable, str(script)] + [str(value) for value in (args or [])]
    environment = os.environ.copy()
    inherited_pythonpath = environment.get("PYTHONPATH")
    pythonpath_entries = [str(SIMULATOR_ROOT), str(ROOT)]
    if inherited_pythonpath:
        pythonpath_entries.append(inherited_pythonpath)
    environment["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    result = subprocess.run(
        command,
        cwd=flow.PROJECT_ROOT,
        capture_output=True,
        text=True,
        env=environment,
    )
    if result.returncode:
        raise RuntimeError(
            f"{event} failed (exit {result.returncode})\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    flow.update_context_after_static_event(event, context)
    return {"event": event, "status": "SUCCESS", "stdout": result.stdout}


def _run_inbound(context):
    report = []
    for event, script in flow.INBOUND_FLOW:
        if event in {"StockIncreased", "InventoryPutaway"}:
            inventory_ids = context.get("current_inventory_ids", [])
            if not inventory_ids:
                raise RuntimeError(f"No inventory IDs available for {event}")
            for inventory_id in inventory_ids:
                context["current_inventory_id"] = inventory_id
                report.append(_execute(event, script, context, [inventory_id]))
            continue
        report.append(_execute(
            event,
            script,
            context,
            flow.build_static_event_args(event, context),
        ))
    return report


def _run_outbound(context):
    _seed_outbound_inventory(context)
    report = []
    for event, script in flow.OUTBOUND_INITIAL_FLOW:
        report.append(_execute(
            event,
            script,
            context,
            [context["correlation_id"]]
            if event == "OrderCreated"
            else flow.build_static_event_args(event, context),
        ))

    context["picking_task_ids"] = flow.get_picking_task_ids(context["order_id"])
    for task_id in context["picking_task_ids"]:
        context["current_picking_task"] = task_id
        if flow.get_task_status(task_id) == "CREATED":
            report.append(_execute(
                "PickingTaskStarted",
                flow.PICKING_STARTED_FILE,
                context,
                [task_id],
            ))
        report.append(_execute(
            "PickingCompleted",
            flow.PICKING_COMPLETED_FILE,
            context,
            [task_id],
        ))

    report.append(_execute(
        "PackingTaskCreated",
        flow.PACKING_CREATED_FILE,
        context,
        [context["order_id"]],
    ))
    packing_task_id = flow.get_packing_task_for_order(context["order_id"])
    if not packing_task_id:
        raise RuntimeError("PackingTaskCreated did not persist a packing task")
    context["packing_task_ids"] = [packing_task_id]
    for task_id in context["packing_task_ids"]:
        context["current_packing_task"] = task_id
        if flow.get_task_status(task_id) == "CREATED":
            report.append(_execute(
                "PackingTaskStarted",
                flow.PACKING_STARTED_FILE,
                context,
                [task_id],
            ))
        report.append(_execute(
            "PackingCompleted",
            flow.PACKING_COMPLETED_FILE,
            context,
            [task_id],
        ))

    context["package_ids"] = flow.get_packed_package_ids(context["order_id"])
    for package_id in context["package_ids"]:
        context["current_package_id"] = package_id
        for event, script in flow.TRANSPORTATION_FLOW:
            report.append(_execute(
                event,
                script,
                context,
                flow.build_static_event_args(event, context),
            ))
    return report


def _seed_outbound_inventory(context):
    now = get_simulation_now()
    correlation_id = context["correlation_id"]
    with Database() as db:
        location = db.fetch_one(
            """SELECT warehouse_id, location_id
               FROM warehouse_locations
               WHERE status = 'ACTIVE'
               ORDER BY warehouse_id, location_id
               LIMIT 1"""
        )
        products = db.fetch_all(
            """SELECT product_id
               FROM products
               WHERE selling_price IS NOT NULL
               ORDER BY product_id
               LIMIT 8"""
        )
        if not location or len(products) < 4:
            raise RuntimeError("Outbound-only run requires an active location and at least four priced products")
        for product in products:
            db.execute(
                """INSERT INTO inventory
                    (product_id, warehouse_id, on_hand_quantity, reserved_quantity,
                     damaged_quantity, inventory_status, last_updated_at, location_id,
                     correlation_id)
                    VALUES (%s,%s,1000,0,0,'AVAILABLE',%s,%s,%s)
                    ON CONFLICT (product_id, warehouse_id) DO NOTHING""",
                (
                    product["product_id"],
                    location["warehouse_id"],
                    now,
                    location["location_id"],
                    correlation_id,
                ),
            )


def _seed_ready_shipment(context):
    correlation_id = context["correlation_id"]
    now = get_simulation_now()
    suffix = uuid.uuid4().hex[:12].upper()
    order_id = f"TORDER-{suffix}"
    package_id = f"TPKG-{suffix}"
    fulfillment_id = f"TFUL-{suffix}"
    shipment_id = f"TSHIP-{suffix}"

    with Database() as db:
        customer = db.fetch_one("SELECT customer_id, city FROM customers ORDER BY customer_id LIMIT 1")
        warehouse = db.fetch_one("SELECT warehouse_id FROM warehouses ORDER BY warehouse_id LIMIT 1")
        product = db.fetch_one("SELECT product_id, selling_price FROM products ORDER BY product_id LIMIT 1")
        if not customer or not warehouse or not product:
            raise RuntimeError("Transportation-only run requires customer, warehouse, and product master data")

        unit_price = product.get("selling_price") or 0
        db.execute(
            """INSERT INTO orders
                (order_id, customer_id, warehouse_id, order_status, total_items,
                 total_quantity, total_amount, currency, order_date, correlation_id, items_created)
                VALUES (%s,%s,%s,'READY',1,1,%s,'USD',%s,%s,TRUE)""",
            (order_id, customer["customer_id"], warehouse["warehouse_id"], unit_price, now, correlation_id),
        )
        db.execute(
            """INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
                VALUES (%s,%s,1,%s,%s)""",
            (order_id, product["product_id"], unit_price, unit_price),
        )
        db.execute(
            """INSERT INTO packages
                (package_id, order_id, warehouse_id, total_items, total_quantity,
                 package_status, packed_at, correlation_id)
                VALUES (%s,%s,%s,1,1,'PACKED',%s,%s)""",
            (package_id, order_id, warehouse["warehouse_id"], now, correlation_id),
        )
        db.execute(
            """INSERT INTO package_items (package_id, product_id, quantity)
                VALUES (%s,%s,1)""",
            (package_id, product["product_id"]),
        )
        db.execute(
            """INSERT INTO outbound_fulfillment
                (fulfillment_id, order_id, warehouse_id, status, correlation_id)
                VALUES (%s,%s,%s,'READY',%s)""",
            (fulfillment_id, order_id, warehouse["warehouse_id"], correlation_id),
        )
        db.execute(
            """INSERT INTO outbound_shipments
                (shipment_id, fulfillment_id, order_id, package_id, destination_city,
                 shipment_status, shipment_date, expected_delivery, created_at, updated_at,
                 correlation_id)
                VALUES (%s,%s,%s,%s,%s,'READY',%s,%s,%s,%s,%s)""",
            (
                shipment_id,
                fulfillment_id,
                order_id,
                package_id,
                customer.get("city"),
                now,
                now + timedelta(days=3),
                now,
                now,
                correlation_id,
            ),
        )

    context.update({
        "order_id": order_id,
        "current_package_id": package_id,
        "current_outbound_shipment_id": shipment_id,
    })
    return shipment_id


def _run_transportation(context):
    shipment_id = _seed_ready_shipment(context)
    report = []
    events = (
        ("CarrierAssigned", "generators/transportation/carrier_assigned.py"),
        ("ShipmentPickedUp", "generators/transportation/shipment_picked_up.py"),
        ("ShipmentInTransit", "generators/transportation/shipment_in_transit.py"),
        ("ShipmentDelivered", "generators/transportation/shipment_delivered.py"),
    )
    for event, script in events:
        report.append(_execute(event, script, context, [shipment_id]))
    return report


def run_domain_flow(domain):
    domain = str(domain).strip().lower()
    if domain not in {"inbound", "outbound", "transportation"}:
        raise ValueError(f"Unsupported domain: {domain}")
    correlation_id = os.getenv("SIMULATION_CORRELATION_ID") or str(uuid.uuid4())
    os.environ["SIMULATION_CORRELATION_ID"] = correlation_id
    context = _new_context()
    context["correlation_id"] = correlation_id

    if domain == "inbound":
        events = _run_inbound(context)
    elif domain == "outbound":
        events = _run_outbound(context)
    else:
        events = _run_transportation(context)
    return {
        "domain": domain,
        "correlation_id": correlation_id,
        "events": events,
        "order_id": context.get("order_id"),
        "shipment_id": context.get("current_outbound_shipment_id"),
    }


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python domain_flow_runner.py inbound|outbound|transportation")
    import json

    print(json.dumps(run_domain_flow(sys.argv[1]), indent=2, default=str))