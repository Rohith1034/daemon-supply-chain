from datetime import datetime, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from master_simulator import run_simulation_cycle


def _purchase_order_approved(queue):
    return queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-DUPLICATE-001",
        correlation_id="corr-duplicate",
        payload={
            "po_id": "PO-DUPLICATE-001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
        },
        scheduled_time=datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc),
    )


def test_duplicate_purchase_order_approved_generation_is_deduplicated():
    queue = EventQueue(memory_mode=True)

    first_id = _purchase_order_approved(queue)
    second_id = _purchase_order_approved(queue)

    assert second_id == first_id
    assert len(queue._memory_events) == 1


def test_duplicate_executor_execution_is_skipped():
    queue = EventQueue(memory_mode=True)
    event_id = _purchase_order_approved(queue)
    event = queue.list_ready_events("2026-09-20T12:00:00+00:00")[0]
    executor = EventExecutor(queue=queue, memory_mode=True)
    calls = []

    def handler(_context):
        calls.append(event_id)
        return {"status": "SUCCESS", "created_events": []}

    first = executor.execute(event, registry={"PurchaseOrderApproved": handler})
    second = executor.execute(event, registry={"PurchaseOrderApproved": handler})

    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
    assert calls == [event_id]


def test_scheduler_restart_does_not_execute_completed_business_event_twice(tmp_path):
    queue = EventQueue(memory_mode=True)
    _purchase_order_approved(queue)
    checkpoint_path = str(tmp_path / "checkpoint.json")
    simulation_time = "2026-09-20T12:00:00+00:00"
    calls = []

    def handler(_context):
        calls.append("executed")
        return {"status": "SUCCESS", "created_events": []}

    first = run_simulation_cycle(
        simulation_time=simulation_time,
        queue=queue,
        checkpoint_path=checkpoint_path,
        registry={"PurchaseOrderApproved": handler},
    )
    second = run_simulation_cycle(
        simulation_time=simulation_time,
        queue=queue,
        checkpoint_path=checkpoint_path,
        registry={"PurchaseOrderApproved": handler},
    )

    assert first["processed_events"][0]["status"] == "COMPLETED"
    assert second["processed_events"] == []
    assert calls == ["executed"]