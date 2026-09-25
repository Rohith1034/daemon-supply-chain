import random
from datetime import datetime, timedelta, timezone

from core.simulation_clock import get_simulation_now


EVENT_DELAY_WINDOWS = {
    "PurchaseOrderCreated": {"min_minutes": 0, "max_minutes": 0},
    "PurchaseOrderApproved": {"min_minutes": 120, "max_minutes": 360},
    "SupplierShipmentCreated": {"min_minutes": 120, "max_minutes": 1440},
    "ASNReceived": {"min_minutes": 60, "max_minutes": 300},
    "SupplierShipmentDelivered": {"min_minutes": 60, "max_minutes": 1440},
    "ReceivingTaskCreated": {"min_minutes": 0, "max_minutes": 120},
    "ReceivingTaskStarted": {"min_minutes": 0, "max_minutes": 60},
    "GoodsReceived": {"min_minutes": 0, "max_minutes": 120},
    "StockIncreased": {"min_minutes": 0, "max_minutes": 60},
    "InventoryPutaway": {"min_minutes": 0, "max_minutes": 60},
    "OrderCreated": {"min_minutes": 0, "max_minutes": 0},
    "OrderItemCreated": {"min_minutes": 0, "max_minutes": 60},
    "InventoryAllocationCreated": {"min_minutes": 0, "max_minutes": 120},
    "InventoryReserved": {"min_minutes": 0, "max_minutes": 120},
    "PickingTaskCreated": {"min_minutes": 0, "max_minutes": 60},
    "ShipmentReady": {"min_minutes": 0, "max_minutes": 60},
    "CarrierAssigned": {"min_minutes": 0, "max_minutes": 60},
    "ShipmentPickedUp": {"min_minutes": 0, "max_minutes": 120},
    "ShipmentInTransit": {"min_minutes": 120, "max_minutes": 720},
    "ShipmentDelivered": {"min_minutes": 60, "max_minutes": 1440},
    "InventoryReceived": {"min_minutes": 0, "max_minutes": 120},
    "InventoryAdjusted": {"min_minutes": 0, "max_minutes": 60},
    "CycleCountCreated": {"min_minutes": 0, "max_minutes": 120},
    "CycleCountCompleted": {"min_minutes": 0, "max_minutes": 120},
    "TaskStarted": {"min_minutes": 0, "max_minutes": 60},
    "PackingTaskCreated": {"min_minutes": 0, "max_minutes": 60},
    "PackingTaskStarted": {"min_minutes": 0, "max_minutes": 60},
    "PackingCompleted": {"min_minutes": 0, "max_minutes": 60},
    "PickingCompleted": {"min_minutes": 0, "max_minutes": 60},
    "DriverAssigned": {"min_minutes": 0, "max_minutes": 60},
    "VehicleAssigned": {"min_minutes": 0, "max_minutes": 60},
    "ShipmentLoaded": {"min_minutes": 0, "max_minutes": 60},
    "CheckpointReached": {"min_minutes": 0, "max_minutes": 180},
    "ShipmentDispatched": {"min_minutes": 0, "max_minutes": 120},
    "ShipmentArrived": {"min_minutes": 30, "max_minutes": 720},
    "GoodsReceived": {"min_minutes": 0, "max_minutes": 120},
}


def get_event_delay(event_type):
    window = EVENT_DELAY_WINDOWS.get(event_type, {"min_minutes": 5, "max_minutes": 30})
    min_minutes = int(window.get("min_minutes", 0))
    max_minutes = int(window.get("max_minutes", min_minutes))
    if max_minutes < min_minutes:
        raise ValueError(f"Invalid delay window for {event_type}: {window!r}")
    delay_minutes = random.randint(min_minutes, max_minutes)
    if delay_minutes <= 0:
        delay_minutes = max(1, min_minutes if min_minutes > 0 else 1)
    return timedelta(minutes=delay_minutes)


def _coerce_datetime(value):
    if value is None:
        return get_simulation_now()
    if isinstance(value, str):
        value = value.replace("Z", "+00:00")
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_future_event_time(event_type, *, base_time=None):
    reference_time = _coerce_datetime(base_time if base_time is not None else get_simulation_now())
    return reference_time + get_event_delay(event_type)
