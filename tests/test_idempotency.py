import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue


def test_duplicate_event_execution_is_skipped_after_completion():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    event_row = {
        "event_id": "evt-idempotent-1",
        "event_type": "PurchaseOrderCreated",
        "aggregate_type": "purchase_orders",
        "aggregate_id": "PO-42",
        "correlation_id": "corr-42",
        "payload": {
            "po_id": "PO-42",
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
    assert second["reason"] == "already processed"
