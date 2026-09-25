import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

import pytest

from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from event_handlers.purchase_order_created import handle


def _event(event_id="PO-IDENTITY-001", payload_id=None):
    payload_id = payload_id or event_id
    return {
        "event_id": "evt-identity-001",
        "event_type": "PurchaseOrderCreated",
        "aggregate_type": "purchase_orders",
        "aggregate_id": event_id,
        "correlation_id": "corr-identity-001",
        "payload": {"purchaseOrder": {"poId": payload_id}},
    }


def test_purchase_order_created_preserves_event_aggregate_id(monkeypatch):
    observed = {}

    def generator(context):
        observed["aggregate_id"] = context["aggregate_id"]
        return {"po_id": context["aggregate_id"], "created_events": []}

    monkeypatch.setattr("generators.purchase_order.purchase_order_created.generate_purchase_order", generator)
    result = handle(_event())

    assert result["status"] == "SUCCESS"
    assert observed["aggregate_id"] == "PO-IDENTITY-001"
    assert result["result"]["po_id"] == "PO-IDENTITY-001"


def test_purchase_order_created_rejects_mismatched_identity(monkeypatch):
    called = False

    def generator(_context):
        nonlocal called
        called = True
        return {"po_id": "SHOULD-NOT-BE-USED"}

    monkeypatch.setattr("generators.purchase_order.purchase_order_created.generate_purchase_order", generator)

    with pytest.raises(ValueError, match="PurchaseOrder ID mismatch between event and payload"):
        handle(_event(event_id="PO-IDENTITY-001", payload_id="PO-IDENTITY-002"))

    assert called is False


def test_executor_fails_mismatched_identity_before_handler():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    called = False

    def handler(_context):
        nonlocal called
        called = True
        return {"status": "SUCCESS", "created_events": []}

    event = _event(event_id="PO-IDENTITY-001", payload_id="PO-IDENTITY-002")
    result = executor.execute(event, registry={"PurchaseOrderCreated": handler})

    assert result["status"] == "FAILED"
    assert "PurchaseOrder ID mismatch between event and payload" in result["error"]
    assert called is False


def test_purchase_order_restart_does_not_create_second_record():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    records = {}

    def generator(context):
        po_id = context["aggregate_id"]
        records.setdefault(po_id, {"po_id": po_id, "status": "CREATED"})
        return {"po_id": po_id, "created_events": []}

    registry = {"PurchaseOrderCreated": lambda context: generator(context)}
    event = _event()
    first = executor.execute(event, registry=registry)
    second = executor.execute(event, registry=registry)

    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
    assert list(records) == ["PO-IDENTITY-001"]