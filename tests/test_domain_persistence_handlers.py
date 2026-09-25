import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from event_handlers import EVENT_HANDLER_REGISTRY
from event_handlers.domain_persistence import (
    handle_carrier_assigned,
    handle_customer_order_created,
    handle_inventory_reserved,
    handle_packing_started,
    handle_picking_task_created,
    handle_truck_departed,
    handle_worker_shift_started,
)


def test_persistence_handlers_are_registered_for_business_events():
    expected = {
        "CustomerOrderCreated": handle_customer_order_created,
        "InventoryReserved": handle_inventory_reserved,
        "WorkerShiftStarted": handle_worker_shift_started,
        "PickingTaskCreated": handle_picking_task_created,
        "PackingStarted": handle_packing_started,
        "CarrierAssigned": handle_carrier_assigned,
        "TruckDeparted": handle_truck_departed,
    }

    for event_type, handler in expected.items():
        assert EVENT_HANDLER_REGISTRY[event_type] is handler