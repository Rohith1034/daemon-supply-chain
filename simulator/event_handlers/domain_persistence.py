from datetime import timezone
import uuid

from core.db import Database
from core.simulation_clock import get_simulation_now
from core.event_timing import get_future_event_time


def _payload(context):
    return dict((context or {}).get("payload") or {})


def _id(context, *fields):
    context = dict(context or {})
    payload = _payload(context)
    for field in fields:
        value = context.get(field) or payload.get(field)
        if value is not None:
            return str(value)
    return str(context.get("aggregate_id")) if context.get("aggregate_id") else None


def _correlation(context):
    return str((context or {}).get("correlation_id") or _payload(context).get("correlation_id") or uuid.uuid4())


def _success(**result):
    return {"status": "SUCCESS", "created_events": [], **result}


def _next_event(event_type, aggregate_type, aggregate_id, correlation_id, payload):
    scheduled_time = get_future_event_time(event_type, base_time=get_simulation_now())
    return {
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": str(aggregate_id),
        "correlation_id": correlation_id,
        "scheduled_time": scheduled_time.isoformat(),
        "payload": {
            **payload,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": str(aggregate_id),
            "correlation_id": correlation_id,
            "event_scheduled_time": scheduled_time.isoformat(),
        },
    }


def handle_customer_order_created(context):
    order_id = _id(context, "order_id", "aggregate_id")
    correlation_id = _correlation(context)
    now = get_simulation_now()
    with Database() as db:
        existing = db.fetch_one("SELECT order_id FROM orders WHERE order_id=%s", (order_id,))
        if existing:
            return _success(order_id=order_id, correlation_id=correlation_id)
        customer = db.fetch_one("SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1")
        warehouse = db.fetch_one("SELECT warehouse_id FROM warehouses ORDER BY warehouse_id LIMIT 1")
        product = db.fetch_one("SELECT product_id, selling_price FROM products ORDER BY product_id LIMIT 1")
        if not customer or not warehouse or not product:
            raise RuntimeError("CustomerOrderCreated requires customer, warehouse, and product master data")
        db.execute(
            """
            INSERT INTO orders (order_id, customer_id, warehouse_id, order_status,
                                total_items, total_quantity, total_amount, currency,
                                order_date, correlation_id, items_created)
            VALUES (%s,%s,%s,'CREATED',1,1,%s,'USD',%s,%s,TRUE)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (order_id, customer["customer_id"], warehouse["warehouse_id"], product["selling_price"] or 0, now, correlation_id),
        )
        db.execute(
            """
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
            VALUES (%s,%s,1,%s,%s)
            """,
            (order_id, product["product_id"], product["selling_price"] or 0, product["selling_price"] or 0),
        )
    items = [{"product_id": str(product["product_id"]), "quantity": 1}]
    return {
        "status": "SUCCESS",
        "created_events": [_next_event(
            "InventoryReserved", "inventory", order_id, correlation_id,
            {"order_id": order_id, "warehouse_id": str(warehouse["warehouse_id"]), "items": items},
        )],
        "order_id": order_id,
        "correlation_id": correlation_id,
    }


def handle_inventory_reserved(context):
    payload = _payload(context)
    order_id = _id(context, "order_id", "aggregate_id")
    warehouse_id = payload.get("warehouse_id")
    items = payload.get("items") or [{"product_id": payload.get("product_id"), "reserved_quantity": payload.get("quantity", 1)}]
    correlation_id = _correlation(context)
    now = get_simulation_now()
    with Database() as db:
        reserved = []
        for item in items:
            product_id = item.get("product_id")
            if product_id is None:
                continue
            quantity = int(item.get("reserved_quantity") or item.get("quantity") or 1)

            existing_allocation = db.fetch_one(
                """
                SELECT allocation_id, inventory_id, location_id
                FROM inventory_allocations
                WHERE order_id=%s AND product_id=%s AND allocation_status IN ('ALLOCATED', 'RESERVED')
                ORDER BY allocated_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (order_id, product_id),
            )

            if existing_allocation is None:
                inventory = db.fetch_one(
                    "SELECT inventory_id, product_id, warehouse_id, available_quantity, location_id FROM inventory WHERE product_id=%s AND warehouse_id=%s ORDER BY inventory_id LIMIT 1 FOR UPDATE",
                    (product_id, warehouse_id),
                )
                if not inventory:
                    db.execute(
                        """
                        INSERT INTO inventory (product_id, warehouse_id, on_hand_quantity, reserved_quantity, inventory_status, correlation_id, last_updated_at)
                        VALUES (%s,%s,%s,0,'AVAILABLE',%s,%s)
                        RETURNING inventory_id
                        """,
                        (product_id, warehouse_id, quantity, correlation_id, now),
                    )
                    inventory = db.fetch_one(
                        "SELECT inventory_id, product_id, warehouse_id, available_quantity, location_id FROM inventory WHERE product_id=%s AND warehouse_id=%s ORDER BY inventory_id LIMIT 1 FOR UPDATE",
                        (product_id, warehouse_id),
                    )
                allocation_id = f"ALLOC-{uuid.uuid4().hex[:10].upper()}"
                db.execute(
                    """
                    INSERT INTO inventory_allocations (allocation_id, order_id, warehouse_id, product_id, allocated_quantity, allocation_status, allocated_at, correlation_id, inventory_id, location_id)
                    VALUES (%s,%s,%s,%s,%s,'RESERVED',%s,%s,%s,%s)
                    ON CONFLICT (allocation_id) DO NOTHING
                    """,
                    (allocation_id, order_id, warehouse_id, product_id, quantity, now, correlation_id, inventory["inventory_id"], inventory.get("location_id")),
                )
                existing_allocation = db.fetch_one(
                    """
                    SELECT allocation_id, inventory_id, location_id
                    FROM inventory_allocations
                    WHERE order_id=%s AND product_id=%s AND allocation_status='RESERVED'
                    ORDER BY allocated_at DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (order_id, product_id),
                )

            if existing_allocation is not None:
                db.execute(
                    "UPDATE inventory_allocations SET allocation_status='RESERVED', allocated_at=%s WHERE allocation_id=%s",
                    (now, existing_allocation["allocation_id"]),
                )

            inventory = db.fetch_one(
                "SELECT inventory_id, product_id, warehouse_id, available_quantity, location_id FROM inventory WHERE product_id=%s AND warehouse_id=%s ORDER BY inventory_id LIMIT 1 FOR UPDATE",
                (product_id, warehouse_id),
            )
            if inventory is None:
                inventory_id = None
            else:
                inventory_id = inventory["inventory_id"]
                db.execute(
                    "UPDATE inventory SET reserved_quantity=reserved_quantity+%s, last_updated_at=%s WHERE inventory_id=%s",
                    (quantity, now, inventory_id),
                )

            if inventory_id is not None:
                db.execute(
                    """
                    INSERT INTO inventory_transactions
                        (product_id, warehouse_id, transaction_type, quantity, reference_type, reference_id, correlation_id, inventory_id)
                    SELECT %s,%s,'RESERVE',%s,'ORDER',%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM inventory_transactions WHERE transaction_type='RESERVE' AND reference_type='ORDER' AND reference_id=%s AND inventory_id=%s)
                    """,
                    (product_id, warehouse_id, quantity, order_id, correlation_id, inventory_id, order_id, inventory_id),
                )

            existing_reservation = db.fetch_one(
                "SELECT reservation_id FROM inventory_reservations WHERE order_id=%s AND product_id=%s AND reservation_status='RESERVED' ORDER BY reserved_at DESC LIMIT 1 FOR UPDATE",
                (order_id, product_id),
            )
            if existing_reservation is None:
                db.execute(
                    """
                    INSERT INTO inventory_reservations (reservation_id, order_id, product_id, warehouse_id, quantity, reservation_status, reserved_at, correlation_id)
                    VALUES (%s,%s,%s,%s,%s,'RESERVED',%s,%s)
                    """,
                    (f"RESV-{uuid.uuid4().hex[:10].upper()}", order_id, product_id, warehouse_id, quantity, now, correlation_id),
                )

            reserved.append({"product_id": product_id, "reserved_quantity": quantity})
    return _success(order_id=order_id, warehouse_id=warehouse_id, items=reserved, correlation_id=correlation_id)


def handle_worker_shift_started(context):
    worker_id = _id(context, "worker_id", "aggregate_id")
    with Database() as db:
        worker = db.fetch_one("SELECT worker_id FROM workers WHERE worker_id=%s ORDER BY worker_id LIMIT 1 FOR UPDATE", (worker_id,)) if worker_id else db.fetch_one("SELECT worker_id FROM workers ORDER BY worker_id LIMIT 1 FOR UPDATE")
        if not worker:
            db.execute(
                "INSERT INTO workers (worker_id, employment_status, current_status, shift_start_time) VALUES (%s,'ACTIVE','WORKING',%s)",
                (worker_id, get_simulation_now()),
            )
            return _success(worker_id=worker_id, correlation_id=_correlation(context))
        db.execute("UPDATE workers SET current_status='WORKING', shift_start_time=%s WHERE worker_id=%s", (get_simulation_now(), worker["worker_id"]))
    return _success(worker_id=str(worker["worker_id"]))


def handle_picking_task_created(context):
    payload = _payload(context)
    order_id = _id(context, "order_id", "aggregate_id") or payload.get("order_id")
    correlation_id = _correlation(context)
    with Database() as db:
        existing = db.fetch_one("SELECT task_id FROM warehouse_tasks WHERE order_id=%s AND task_type='PICKING' ORDER BY created_at LIMIT 1", (order_id,))
        if existing:
            return _success(task_id=str(existing["task_id"]), order_id=order_id, correlation_id=correlation_id)

        allocation = db.fetch_one(
            """
            SELECT order_id, warehouse_id, product_id, allocated_quantity, location_id, allocation_id
            FROM inventory_allocations
            WHERE order_id=%s AND allocation_status='RESERVED'
            ORDER BY allocated_at ASC
            LIMIT 1
            FOR UPDATE
            """,
            (order_id,),
        )
        if not allocation:
            raise RuntimeError("PickingTaskCreated requires an existing reserved inventory allocation")
        task_id = f"PICK-{uuid.uuid4().hex[:10].upper()}"
        db.execute(
            """
            INSERT INTO warehouse_tasks (task_id, task_type, warehouse_id, product_id, location, quantity, priority, status, created_at, order_id, allocation_id, correlation_id, expected_quantity)
            VALUES (%s,'PICKING',%s,%s,%s,%s,'NORMAL','CREATED',%s,%s,%s,%s,%s)
            """,
            (task_id, allocation["warehouse_id"], allocation["product_id"], allocation["location_id"], allocation["allocated_quantity"], get_simulation_now(), order_id, allocation["allocation_id"], correlation_id, allocation["allocated_quantity"]),
        )
    return _success(task_id=task_id, order_id=order_id, correlation_id=correlation_id)


def handle_packing_started(context):
    task_id = _id(context, "task_id", "aggregate_id")
    with Database() as db:
        task = db.fetch_one("SELECT task_id FROM warehouse_tasks WHERE task_id=%s ORDER BY created_at LIMIT 1 FOR UPDATE", (task_id,)) if task_id else db.fetch_one("SELECT task_id FROM warehouse_tasks WHERE task_type='PACKING' ORDER BY created_at LIMIT 1 FOR UPDATE")
        if not task:
            raise RuntimeError("PackingStarted requires a packing task")
        db.execute("UPDATE warehouse_tasks SET status='PACKING_STARTED', task_started_at=%s WHERE task_id=%s", (get_simulation_now(), task["task_id"]))
    return _success(task_id=str(task["task_id"]))


def handle_carrier_assigned(context):
    shipment_id = _id(context, "shipment_id", "aggregate_id")
    correlation_id = _correlation(context)
    with Database() as db:
        existing = db.fetch_one("SELECT shipment_id FROM [shipment_transportation WHERE shipment_id=%s LIMIT 1", (shipment_id,))
        if existing:
            return _success(shipment_id=shipment_id, correlation_id=correlation_id)
        shipment = db.fetch_one("SELECT shipment_id, warehouse_id FROM shipments WHERE shipment_id=%s FOR UPDATE", (shipment_id,))
        if not shipment:
            raise RuntimeError("CarrierAssigned requires a shipment row")
        db.execute(
            "INSERT INTO shipment_transportation (id, shipment_id, assigned_at, status) SELECT COALESCE(MAX(id),0)+1,%s,%s,'ASSIGNED' FROM shipment_transportation",
            (shipment_id, get_simulation_now()),
        )
    return _success(shipment_id=shipment_id, correlation_id=correlation_id)


def handle_truck_departed(context):
    shipment_id = _id(context, "shipment_id", "aggregate_id")
    with Database() as db:
        updated = db.fetch_one("UPDATE shipment_transportation SET status='DEPARTED' WHERE shipment_id=%s RETURNING shipment_id", (shipment_id,))
        if not updated:
            raise RuntimeError("TruckDeparted requires a transportation record")
    return _success(shipment_id=shipment_id, correlation_id=_correlation(context))


def handle_outbound_shipment_created(context):
    payload = _payload(context)
    shipment_id = _id(context, "shipment_id", "aggregate_id")
    order_id = _id(context, "order_id") or shipment_id
    correlation_id = _correlation(context)
    now = get_simulation_now()
    with Database() as db:
        existing = db.fetch_one("SELECT shipment_id FROM shipments WHERE shipment_id=%s", (shipment_id,))
        if not existing:
            order = db.fetch_one("SELECT warehouse_id FROM orders WHERE order_id=%s", (order_id,))
            supplier = db.fetch_one("SELECT supplier_id FROM suppliers ORDER BY supplier_id LIMIT 1")
            warehouse_id = payload.get("warehouse_id") or (order["warehouse_id"] if order else None)
            if not warehouse_id or not supplier:
                raise RuntimeError("OutboundShipmentCreated requires warehouse and supplier master data")
            db.execute(
                """
                INSERT INTO shipments (shipment_id, po_id, supplier_id, warehouse_id, shipment_status, shipment_date, expected_delivery, correlation_id)
                VALUES (%s,%s,%s,%s,'CREATED',%s,%s,%s)
                ON CONFLICT (shipment_id) DO NOTHING
                """,
                (shipment_id, order_id, supplier["supplier_id"], warehouse_id, now, now, correlation_id),
            )
    return _success(shipment_id=shipment_id, order_id=order_id, correlation_id=correlation_id)