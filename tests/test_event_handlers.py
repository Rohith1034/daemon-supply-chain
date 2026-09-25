import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core import event_timing
from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from event_handlers.base import build_handler
from event_handlers.asn_received import handle as handle_asn_received
from event_handlers.purchase_order_approved import handle as handle_purchase_order_approved
from event_handlers.purchase_order_created import handle as handle_purchase_order_created
from event_handlers.supplier_shipment_created import handle as handle_supplier_shipment_created


def test_handler_returns_success_status_and_future_event_contract(monkeypatch):
    def fake_generator():
        return {
            "po_id": "PO-9001",
            "correlation_id": "corr-9001",
            "created_events": [{
                "event_type": "FutureDispatchQueued",
                "aggregate_type": "purchase_orders",
                "aggregate_id": "PO-9001",
                "correlation_id": "corr-9001",
                "scheduled_time": "2026-09-20T10:00:00+00:00",
                "payload": {"po_id": "PO-9001", "priority": "HIGH"},
            }],
        }

    monkeypatch.setattr("generators.purchase_order.purchase_order_created.generate_purchase_order", fake_generator)

    result = handle_purchase_order_created({"event_type": "PurchaseOrderCreated", "correlation_id": "corr-9001"})

    assert result["status"] == "SUCCESS"
    assert len(result["created_events"]) == 1
    assert set(result["created_events"][0].keys()) == {"event_type", "aggregate_type", "aggregate_id", "correlation_id", "scheduled_time", "payload"}
    assert result["created_events"][0]["correlation_id"] == "corr-9001"


def test_handler_propagates_correlation_and_aggregate_ids(monkeypatch):
    def fake_generator():
        return {
            "shipment_id": "S-9002",
            "correlation_id": "corr-9002",
            "created_events": [{
                "event_type": "ShipmentReady",
                "aggregate_type": "shipments",
                "aggregate_id": "S-9002",
                "correlation_id": "corr-9002",
                "scheduled_time": "2026-09-20T11:00:00+00:00",
                "payload": {"shipment_id": "S-9002"},
            }],
        }

    monkeypatch.setattr("generators.supplier.supplier_shipment_created.create_supplier_shipment", fake_generator)

    result = handle_supplier_shipment_created({"event_type": "SupplierShipmentCreated", "correlation_id": "corr-9002"})

    assert result["status"] == "SUCCESS"
    assert result["created_events"][0]["aggregate_id"] == "S-9002"
    assert result["created_events"][0]["correlation_id"] == "corr-9002"


def test_handler_executes_without_direct_chaining(monkeypatch):
    def fake_generator():
        return {"po_id": "PO-9003", "correlation_id": "corr-9003"}

    monkeypatch.setattr("generators.purchase_order.purchase_order_approved.approve_purchase_order", fake_generator)

    result = handle_purchase_order_approved({"event_type": "PurchaseOrderApproved", "correlation_id": "corr-9003"})

    assert result["status"] == "SUCCESS"
    assert result["created_events"] == []


def test_build_handler_passes_scalar_ids_as_keywords(monkeypatch):
    def fake_generator(task_id=None, order_id=None):
        assert task_id == "TASK-10"
        assert order_id == "ORDER-20"
        return {"task_id": task_id, "order_id": order_id}

    monkeypatch.setattr("generators.warehouse.goods_received.generate_goods_received", fake_generator)
    handle = build_handler("generators.warehouse.goods_received.generate_goods_received", event_type="GoodsReceived", aggregate_type="warehouse_tasks", aggregate_id_field="task_id")

    result = handle({
        "event_type": "GoodsReceived",
        "aggregate_id": "TASK-10",
        "task_id": "TASK-10",
        "order_id": "ORDER-20",
        "correlation_id": "corr-9005",
    })

    assert result["status"] == "SUCCESS"
    assert result["result"]["task_id"] == "TASK-10"


def test_supplier_shipment_handler_prefers_shipment_id_over_po_id(monkeypatch):
    seen = {}

    def fake_generator(event_context=None, count=1):
        seen["aggregate_id"] = event_context.get("aggregate_id")
        seen["payload_po_id"] = (event_context.get("payload") or {}).get("po_id")
        seen["payload_shipment_id"] = (event_context.get("payload") or {}).get("shipment_id")
        return {
            "shipment_id": "SHIP-2026",
            "po_id": "PO-2026",
            "correlation_id": "corr-2026",
        }

    monkeypatch.setattr("generators.supplier.supplier_shipment_created.create_supplier_shipment", fake_generator)

    result = handle_supplier_shipment_created({
        "event_type": "SupplierShipmentCreated",
        "aggregate_id": "PO-2026",
        "correlation_id": "corr-2026",
        "payload": {"po_id": "PO-2026", "shipment_id": "SHIP-2026", "correlation_id": "corr-2026"},
    })

    assert result["status"] == "SUCCESS"
    assert seen["aggregate_id"] == "SHIP-2026"
    assert seen["payload_po_id"] == "PO-2026"
    assert seen["payload_shipment_id"] == "SHIP-2026"


def test_asn_received_handler_prefers_shipment_id_over_po_id(monkeypatch):
    seen = {}

    def fake_generator(event_context=None):
        seen["aggregate_id"] = event_context.get("aggregate_id")
        seen["payload_po_id"] = (event_context.get("payload") or {}).get("po_id")
        seen["payload_shipment_id"] = (event_context.get("payload") or {}).get("shipment_id")
        return {
            "shipment_id": "SHIP-9001",
            "po_id": "PO-9001",
            "correlation_id": "corr-9001",
        }

    monkeypatch.setattr("generators.purchase_order.asn_received.generate_asn_received", fake_generator)

    result = handle_asn_received({
        "event_type": "ASNReceived",
        "aggregate_id": "PO-9001",
        "correlation_id": "corr-9001",
        "payload": {"po_id": "PO-9001", "shipment_id": "SHIP-9001", "correlation_id": "corr-9001"},
    })

    assert result["status"] == "SUCCESS"
    assert seen["aggregate_id"] == "SHIP-9001"
    assert seen["payload_po_id"] == "PO-9001"
    assert seen["payload_shipment_id"] == "SHIP-9001"


def test_event_timing_returns_future_timestamp(monkeypatch):
    fixed_now = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(event_timing, "get_simulation_now", lambda: fixed_now)
    monkeypatch.setattr(event_timing.random, "randint", lambda start, end: start)

    result = event_timing.get_future_event_time("PurchaseOrderApproved")

    assert result > fixed_now
    assert result == fixed_now + timedelta(hours=2)


def test_event_timing_respects_different_sla_windows(monkeypatch):
    base_time = datetime(2026, 9, 20, 8, 0, tzinfo=timezone.utc)

    def fake_randint(start, end):
        mapping = {
            (120, 360): 120,
            (120, 1440): 240,
        }
        return mapping.get((start, end), start)

    monkeypatch.setattr(event_timing.random, "randint", fake_randint)

    approval_time = event_timing.get_future_event_time("PurchaseOrderApproved", base_time=base_time)
    shipment_time = event_timing.get_future_event_time("SupplierShipmentCreated", base_time=base_time)

    assert approval_time == base_time + timedelta(hours=2)
    assert shipment_time == base_time + timedelta(hours=4)


def test_duplicate_event_execution_is_skipped_by_executor():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    event_row = {
        "event_id": "evt-dup-1",
        "event_type": "PurchaseOrderCreated",
        "aggregate_type": "purchase_orders",
        "aggregate_id": "PO-9004",
        "correlation_id": "corr-9004",
        "status": "PENDING",
        "payload": {
            "po_id": "PO-9004",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
    }

    registry = {"PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []}}

    first = executor.execute(event_row, registry=registry)
    second = executor.execute(event_row, registry=registry)

    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
