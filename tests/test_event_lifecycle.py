import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from master_simulator import _coerce_future_event


def test_event_timestamp_lifecycle_order_is_created_started_completed():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    event_row = {
        "event_id": "evt-lifecycle-1",
        "event_type": "PurchaseOrderCreated",
        "aggregate_type": "purchase_orders",
        "aggregate_id": "PO-5001",
        "correlation_id": "corr-5001",
        "event_created_time": "2026-09-20T10:00:00+00:00",
        "scheduled_time": "2026-09-20T10:00:00+00:00",
        "payload": {
            "po_id": "PO-5001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
    }

    result = executor.execute(event_row, registry={"PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []}})

    assert result["status"] == "COMPLETED"
    assert event_row["event_created_time"] <= event_row["event_started_time"]
    assert event_row["event_started_time"] <= event_row["event_completed_time"]


def test_correlation_and_aggregate_ids_are_preserved_across_lifecycle():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)

    root = {
        "event_id": "evt-root-1",
        "event_type": "PurchaseOrderCreated",
        "aggregate_type": "purchase_orders",
        "aggregate_id": "PO-6001",
        "correlation_id": "corr-6001",
        "event_created_time": "2026-09-20T10:00:00+00:00",
        "scheduled_time": "2026-09-20T10:00:00+00:00",
        "payload": {
            "po_id": "PO-6001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
    }

    result = executor.execute(root, registry={"PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": [{"event_type": "PurchaseOrderApproved", "aggregate_type": "purchase_orders", "aggregate_id": "PO-6001", "correlation_id": "corr-6001", "scheduled_time": "2026-09-20T11:00:00+00:00", "payload": {"po_id": "PO-6001", "priority": "HIGH"}}]}})

    assert result["correlation_id"] == "corr-6001"
    assert result["aggregate_id"] == "PO-6001"
    assert result["result"]["created_events"][0]["correlation_id"] == "corr-6001"
    assert result["result"]["created_events"][0]["aggregate_id"] == "PO-6001"


def test_future_event_coercion_prefers_real_shipment_id_over_po_id():
    event_spec = {
        "event_type": "SupplierShipmentCreated",
        "aggregate_type": "shipments",
        "aggregate_id": "PO-7001",
        "correlation_id": "corr-7001",
        "payload": {
            "po_id": "PO-7001",
            "shipment_id": "SHIP-7001",
            "task_id": "TASK-7001",
            "priority": "HIGH",
        },
    }

    normalized = _coerce_future_event(event_spec)

    assert normalized is not None
    assert normalized["aggregate_id"] == "SHIP-7001"
    assert normalized["payload"]["shipment_id"] == "SHIP-7001"


def test_supplier_shipment_created_event_uses_real_shipment_id_for_downstream_events():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)

    event_row = {
        "event_id": "evt-shipment-aggregate-1",
        "event_type": "SupplierShipmentCreated",
        "aggregate_type": "shipments",
        "aggregate_id": "PO-8001",
        "correlation_id": "corr-8001",
        "event_created_time": "2026-09-20T10:00:00+00:00",
        "scheduled_time": "2026-09-20T10:00:00+00:00",
        "payload": {
            "po_id": "PO-8001",
            "shipment_id": "SHIP-8001",
            "entity_type": "Shipment",
            "current_state": "CREATED",
            "next_state": "READY",
            "priority": "HIGH",
        },
    }

    result = executor.execute(
        event_row,
        registry={
            "SupplierShipmentCreated": lambda context: {
                "status": "SUCCESS",
                "shipment_id": "SHIP-8001",
                "po_id": "PO-8001",
                "created_events": [{
                    "event_type": "ASNReceived",
                    "aggregate_type": "shipments",
                    "payload": {
                        "po_id": "PO-8001",
                        "shipment_id": "SHIP-8001",
                        "priority": "MEDIUM",
                    },
                }],
            }
        },
    )

    assert result["status"] == "COMPLETED"
    assert result["result"]["created_events"][0]["aggregate_id"] == "SHIP-8001"
    assert result["result"]["created_events"][0]["payload"]["aggregate_id"] == "SHIP-8001"
