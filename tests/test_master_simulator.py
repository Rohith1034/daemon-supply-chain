import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.checkpointing import SimulationCheckpoint
from core.event_scheduler.event_queue import EventQueue
from master_simulator import _coerce_future_event, run_simulation_cycle


def test_master_simulator_runs_cycle_and_persists_checkpoint(tmp_path):
    queue = EventQueue(memory_mode=True)
    checkpoint_path = tmp_path / "checkpoint.json"
    now = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)

    queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-9001",
        correlation_id="corr-9001",
        payload={
            "po_id": "PO-9001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time=now.isoformat(),
        priority="HIGH",
    )

    registry = {"PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []}}

    result = run_simulation_cycle(
        simulation_time=now,
        queue=queue,
        checkpoint_path=str(checkpoint_path),
        registry=registry,
    )

    assert result["processed_events"][0]["status"] == "COMPLETED"
    checkpoint = SimulationCheckpoint(file_path=str(checkpoint_path))
    assert checkpoint.read() == now.astimezone(timezone.utc)

    repeat = run_simulation_cycle(
        simulation_time=now + timedelta(minutes=5),
        queue=queue,
        checkpoint_path=str(checkpoint_path),
        registry=registry,
    )

    assert repeat["ready_events"] == []


def test_failed_cycle_does_not_advance_checkpoint(tmp_path):
    queue = EventQueue(memory_mode=True)
    checkpoint_path = tmp_path / "failed-checkpoint.json"
    now = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)

    queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-FAILED-1",
        correlation_id="corr-failed-1",
        payload={
            "po_id": "PO-FAILED-1",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time=now.isoformat(),
        priority="HIGH",
    )

    result = run_simulation_cycle(
        simulation_time=now,
        queue=queue,
        checkpoint_path=str(checkpoint_path),
        registry={
            "PurchaseOrderCreated": lambda _context: (_ for _ in ()).throw(
                RuntimeError("forced failure")
            )
        },
    )

    assert result["processed_events"][0]["status"] == "RETRY_SCHEDULED"
    assert SimulationCheckpoint(file_path=str(checkpoint_path)).read() is None


def test_coerce_future_event_raises_if_same_time_but_normalizes_to_next_tick():
    base_time = datetime(2026, 9, 20, 9, 0, tzinfo=timezone.utc)

    event = _coerce_future_event(
        {
            "event_type": "PickingCompleted",
            "aggregate_type": "warehouse_tasks",
            "aggregate_id": "TASK-100",
            "correlation_id": "corr-100",
            "payload": {"task_id": "TASK-100"},
            "scheduled_time": base_time.isoformat(),
        },
        default_base_time=base_time,
    )

    assert event["scheduled_time"] > base_time.isoformat()
    assert event["payload"]["event_scheduled_time"] > base_time.isoformat()
