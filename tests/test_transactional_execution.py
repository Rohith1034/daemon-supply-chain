import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue


def _event(event_id, event_type, po_id="PO-TX-001"):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": "purchase_orders",
        "aggregate_id": po_id,
        "correlation_id": f"corr-{po_id}",
        "payload": {
            "po_id": po_id,
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
    }


def test_purchase_order_created_creates_created_status_before_completion():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    purchase_orders = {}

    def create_purchase_order(context):
        purchase_orders[context["aggregate_id"]] = "CREATED"
        return {"status": "SUCCESS", "created_events": []}

    result = executor.execute(
        _event("evt-created", "PurchaseOrderCreated"),
        registry={"PurchaseOrderCreated": create_purchase_order},
    )

    assert result["status"] == "COMPLETED"
    assert purchase_orders["PO-TX-001"] == "CREATED"


def test_purchase_order_approved_changes_status_before_completion():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    purchase_orders = {"PO-TX-001": "CREATED"}

    def approve_purchase_order(context):
        assert purchase_orders[context["aggregate_id"]] == "CREATED"
        purchase_orders[context["aggregate_id"]] = "APPROVED"
        return {"status": "SUCCESS", "created_events": []}

    result = executor.execute(
        _event("evt-approved", "PurchaseOrderApproved"),
        registry={"PurchaseOrderApproved": approve_purchase_order},
    )

    assert result["status"] == "COMPLETED"
    assert purchase_orders["PO-TX-001"] == "APPROVED"


def test_failed_handler_does_not_mark_event_completed():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    event = _event("evt-failed", "PurchaseOrderApproved")
    event["payload"]["max_retry_count"] = 0

    def failed_handler(_context):
        raise RuntimeError("business update failed")

    result = executor.execute(event, registry={"PurchaseOrderApproved": failed_handler})

    assert result["status"] == "FAILED"
    assert event["status"] == "FAILED"
    assert executor._execution_log["evt-failed"]["status"] == "FAILED"


def test_restart_does_not_duplicate_purchase_order_state_transition():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    purchase_orders = {"PO-TX-001": "CREATED"}
    transition_count = 0

    def approve_purchase_order(context):
        nonlocal transition_count
        transition_count += 1
        purchase_orders[context["aggregate_id"]] = "APPROVED"
        return {"status": "SUCCESS", "created_events": []}

    event = _event("evt-restart", "PurchaseOrderApproved")
    first = executor.execute(event, registry={"PurchaseOrderApproved": approve_purchase_order})
    second = executor.execute(event, registry={"PurchaseOrderApproved": approve_purchase_order})

    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
    assert purchase_orders["PO-TX-001"] == "APPROVED"
    assert transition_count == 1