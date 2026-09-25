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


def test_completed_event_is_removed_from_ready_queue_and_added_to_history():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)

    event_id = queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-1001",
        correlation_id="corr-1001",
        payload={"po_id": "PO-1001", "priority": "HIGH"},
        scheduled_time="2026-09-20T10:00:00+00:00",
        priority="HIGH",
    )

    ready = queue.list_ready_events("2026-09-20T10:00:00+00:00")
    result = executor.execute(ready[0], registry={"PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []}})

    assert result["status"] == "COMPLETED"
    assert queue.list_ready_events("2026-09-20T10:00:00+00:00") == []
    assert queue.queue_size == 0
    assert any(item["event_id"] == event_id for item in queue.execution_history)


def test_ready_events_contain_only_executable_rows():
    queue = EventQueue(memory_mode=True)
    queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-2001",
        correlation_id="corr-2001",
        payload={"po_id": "PO-2001", "priority": "HIGH"},
        scheduled_time="2026-09-20T10:00:00+00:00",
        priority="HIGH",
    )
    queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-2002",
        correlation_id="corr-2002",
        payload={"po_id": "PO-2002", "priority": "MEDIUM"},
        scheduled_time="2026-09-20T10:10:00+00:00",
        priority="MEDIUM",
    )

    ready = queue.list_ready_events("2026-09-20T10:00:00+00:00")
    assert len(ready) == 1
    assert ready[0]["aggregate_id"] == "PO-2001"
    assert queue.queue_size == 1
