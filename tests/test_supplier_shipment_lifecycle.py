import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from core.db import Database
from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from event_handlers.supplier_shipment_created import handle


def _event():
    return {
        "event_id": "evt-shipment-001",
        "event_type": "SupplierShipmentCreated",
        "aggregate_type": "shipments",
        "aggregate_id": "PO-SHIP-001",
        "correlation_id": "corr-po-shipment-001",
        "payload": {
            "po_id": "PO-SHIP-001",
            "entity_type": "Shipment",
            "current_state": "CREATED",
            "next_state": "READY",
        },
    }


def test_purchase_order_approved_creates_supplier_shipment_event():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    result = executor.execute(
        {
            "event_id": "evt-approved-001",
            "event_type": "PurchaseOrderApproved",
            "aggregate_type": "purchase_orders",
            "aggregate_id": "PO-SHIP-001",
            "correlation_id": "corr-po-shipment-001",
            "payload": {"po_id": "PO-SHIP-001", "entity_type": "PurchaseOrder", "current_state": "CREATED", "next_state": "APPROVED"},
        },
        registry={"PurchaseOrderApproved": lambda _context: {"status": "SUCCESS", "created_events": []}},
    )

    assert result["status"] == "COMPLETED"
    assert result["result"]["created_events"][0]["event_type"] == "SupplierShipmentCreated"
    assert result["result"]["created_events"][0]["aggregate_id"] == "PO-SHIP-001"


def test_supplier_shipment_handler_uses_event_po_identity(monkeypatch):
    observed = {}

    def generator(context):
        observed.update(context)
        return {"shipment_id": "SHIP-001", "po_id": context["aggregate_id"], "created_events": []}

    monkeypatch.setattr("generators.supplier.supplier_shipment_created.create_supplier_shipment", generator)
    result = handle(_event())

    assert result["status"] == "SUCCESS"
    assert observed["aggregate_id"] == "PO-SHIP-001"


def test_publish_event_deduplicates_shipment_events_by_correlation():
    correlation_id = "corr-publish-dedupe-001"
    with Database() as db:
        db.execute("DELETE FROM event_outbox WHERE correlation_id = %s AND event_type = %s", (correlation_id, "SupplierShipmentCreated"))

        first = __import__("core.outbox", fromlist=["publish_event"]).publish_event(
            db,
            event_type="SupplierShipmentCreated",
            aggregate_type="SHIPMENT",
            aggregate_id="SHIP-PUBLISH-001",
            correlation_id=correlation_id,
            payload={"shipment_id": "SHIP-PUBLISH-001", "po_id": "PO-PUBLISH-001"},
        )
        second = __import__("core.outbox", fromlist=["publish_event"]).publish_event(
            db,
            event_type="SupplierShipmentCreated",
            aggregate_type="SHIPMENT",
            aggregate_id="SHIP-PUBLISH-001",
            correlation_id=correlation_id,
            payload={"shipment_id": "SHIP-PUBLISH-001", "po_id": "PO-PUBLISH-001"},
        )

        row_count = db.fetch_one(
            "SELECT COUNT(*) AS c FROM event_outbox WHERE correlation_id = %s AND event_type = %s",
            (correlation_id, "SupplierShipmentCreated"),
        )

    assert first == second
    assert row_count["c"] == 1


def test_duplicate_supplier_shipment_execution_is_idempotent(monkeypatch):
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    shipments = {}

    def generator(context):
        po_id = context["aggregate_id"]
        shipments.setdefault(po_id, {"po_id": po_id, "shipment_id": "SHIP-001"})
        return {"shipment_id": "SHIP-001", "po_id": po_id, "created_events": []}

    registry = {"SupplierShipmentCreated": lambda context: generator(context)}
    first = executor.execute(_event(), registry=registry)
    second = executor.execute(_event(), registry=registry)

    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
    assert len(shipments) == 1


def test_supplier_shipment_is_not_enqueued_before_purchase_order_approval():
    with Database() as db:
        db.execute("DELETE FROM event_outbox")
        db.commit()

        supplier = db.fetch_one("SELECT supplier_id FROM suppliers LIMIT 1")
        warehouse = db.fetch_one("SELECT warehouse_id FROM warehouses LIMIT 1")
        assert supplier is not None and warehouse is not None

        supplier_id = supplier["supplier_id"]
        warehouse_id = warehouse["warehouse_id"]

        po_id = f"PO-CAUSAL-{uuid.uuid4().hex[:8].upper()}"

        db.execute(
            """
            INSERT INTO purchase_orders (
                po_id, supplier_id, warehouse_id, po_status, order_date, expected_delivery,
                total_items, total_quantity, total_amount, currency, correlation_id
            ) VALUES (%s, %s, %s, %s, NOW(), NOW() + INTERVAL '7 days', %s, %s, %s, %s, %s)
            """,
            (
                po_id,
                supplier_id,
                warehouse_id,
                "CREATED",
                1,
                10,
                100.00,
                "USD",
                str(uuid.uuid4()),
            ),
        )
        db.commit()

        queue = EventQueue(memory_mode=False)
        inserted_id = queue.insert_future_event(
            event_type="SupplierShipmentCreated",
            aggregate_type="shipments",
            aggregate_id="SHIP-CAUSAL-001",
            correlation_id=str(uuid.uuid4()),
            payload={"po_id": po_id, "shipment_id": "SHIP-CAUSAL-001"},
            scheduled_time="2026-09-20T18:32:00+00:00",
        )

        assert inserted_id is None

        ready_events = queue.list_ready_events("2026-09-20T18:32:00+00:00")
        blocked_events = [
            event for event in ready_events
            if event.get("event_type") == "SupplierShipmentCreated"
            and (event.get("aggregate_id") == "SHIP-CAUSAL-001" or event.get("payload", {}).get("po_id") == po_id)
        ]
        assert not blocked_events

        db.execute("DELETE FROM purchase_orders WHERE po_id = %s", (po_id,))
        db.commit()