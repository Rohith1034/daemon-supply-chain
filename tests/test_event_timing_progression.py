import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.event_timing import get_delivery_event_time, get_future_event_time
from core.payloads import utc_timestamp
from core.simulation_clock import get_simulation_now


INBOUND_EVENTS = (
    "PurchaseOrderCreated",
    "PurchaseOrderApproved",
    "SupplierShipmentCreated",
    "ASNReceived",
    "SupplierShipmentDelivered",
    "ReceivingTaskCreated",
    "ReceivingTaskStarted",
    "GoodsReceived",
    "StockIncreased",
    "InventoryPutaway",
)

OUTBOUND_EVENTS = (
    "OrderCreated",
    "OrderItemCreated",
    "InventoryAllocationCreated",
    "InventoryReserved",
    "PickingTaskCreated",
    "PickingTaskStarted",
    "PickingCompleted",
    "PackingTaskCreated",
    "PackingTaskStarted",
    "PackingCompleted",
    "ShipmentReady",
    "CarrierAssigned",
    "ShipmentPickedUp",
    "ShipmentInTransit",
    "ShipmentDelivered",
)

TRANSPORTATION_EVENTS = (
    "CarrierAssigned",
    "ShipmentPickedUp",
    "ShipmentInTransit",
    "ShipmentDelivered",
)


def _assert_chain_progresses(events, *, first_is_root=True):
    parent_time = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)
    timestamps = [parent_time]

    for event in events[1 if first_is_root else 0:]:
        child_time = get_future_event_time(event, base_time=parent_time)
        delta = child_time - parent_time
        assert delta.total_seconds() > 0, (
            f"{event} must occur after its parent at {parent_time}; "
            f"child timestamp was {child_time}"
        )
        timestamps.append(child_time)
        parent_time = child_time

    assert all(parent < child for parent, child in zip(timestamps, timestamps[1:]))


def test_inbound_events_progress_from_each_direct_parent():
    _assert_chain_progresses(INBOUND_EVENTS)


def test_outbound_events_progress_from_each_direct_parent():
    _assert_chain_progresses(OUTBOUND_EVENTS)


def test_transportation_events_progress_from_each_direct_parent():
    _assert_chain_progresses(TRANSPORTATION_EVENTS, first_is_root=False)


def test_outbound_delivery_is_four_to_six_days_after_order(monkeypatch):
    order_time = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)
    last_event_time = datetime(2026, 10, 3, 8, tzinfo=timezone.utc)
    monkeypatch.setattr("core.event_timing.random.randint", lambda low, high: low)

    delivered_at = get_delivery_event_time(
        "outbound",
        parent_time=last_event_time,
        workflow_start=order_time,
    )

    assert last_event_time < delivered_at
    assert 4 <= (delivered_at - order_time).days <= 6


def test_transportation_delivery_is_two_to_seven_days_after_parent(monkeypatch):
    parent_time = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)
    monkeypatch.setattr("core.event_timing.random.randint", lambda low, high: high)

    delivered_at = get_delivery_event_time(
        "transportation",
        parent_time=parent_time,
    )

    assert delivered_at > parent_time
    assert 2 <= (delivered_at - parent_time).days <= 7


def test_payload_timestamps_use_the_simulation_clock(monkeypatch):
    expected = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)
    monkeypatch.setenv("SIMULATION_NOW", expected.isoformat())

    assert get_simulation_now() == expected
    assert datetime.fromisoformat(utc_timestamp()) == expected
