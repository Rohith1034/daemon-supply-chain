import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.event_scheduler.event_queue import EventQueue


def test_future_events_are_sorted_by_priority_then_schedule_time():
    queue = EventQueue(memory_mode=True)
    base_time = datetime(2026, 9, 20, 8, 0, tzinfo=timezone.utc)

    queue.insert_future_event(
        event_type="LowPriorityEvent",
        aggregate_type="scheduled_events",
        aggregate_id="A-1",
        correlation_id="corr-a",
        payload={"priority": "LOW"},
        scheduled_time=base_time + timedelta(hours=1),
        priority="LOW",
    )
    queue.insert_future_event(
        event_type="HighPriorityEvent",
        aggregate_type="scheduled_events",
        aggregate_id="B-1",
        correlation_id="corr-b",
        payload={"priority": "HIGH"},
        scheduled_time=base_time + timedelta(hours=5),
        priority="HIGH",
    )
    queue.insert_future_event(
        event_type="MediumPriorityEvent",
        aggregate_type="scheduled_events",
        aggregate_id="C-1",
        correlation_id="corr-c",
        payload={"priority": "MEDIUM"},
        scheduled_time=base_time + timedelta(hours=2),
        priority="MEDIUM",
    )

    ready = queue.list_ready_events(simulation_time=base_time + timedelta(hours=10))
    assert [row["event_type"] for row in ready] == [
        "HighPriorityEvent",
        "MediumPriorityEvent",
        "LowPriorityEvent",
    ]
