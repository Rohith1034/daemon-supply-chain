from __future__ import annotations

from typing import Iterable

VALID_RUN_WINDOWS = ("morning", "afternoon", "evening", "night")

LMS_EVENT_TYPES = {
    "PunchIn",
    "PunchOut",
    "ShiftStart",
    "ShiftEnd",
    "BreakStart",
    "BreakEnd",
    "WorkerCheckIn",
    "WorkerCheckOut",
}

CONTRACT_BY_WINDOW = {
    "morning": {
        "name": "morning",
        "domain": "inbound",
        "allowed_events": [
            "PurchaseOrderCreated",
            "PurchaseOrderApproved",
            "SupplierShipmentCreated",
            "ASNReceived",
            "SupplierShipmentDelivered",
            "ReceivingInboundTaskCreated",
            "ReceivingTaskCreated",
            "ReceivingTaskStarted",
            "ReceivingTaskCompleted",
            "GoodsReceived",
            "StockIncreased",
            "InventoryPutaway",
        ],
        "root_event": "PurchaseOrderCreated",
        "lms_events": list(LMS_EVENT_TYPES),
    },
    "afternoon": {
        "name": "afternoon",
        "domain": "outbound",
        "allowed_events": [
            "OrderCreated",
            "OrderItemCreated",
            "InventoryAllocationCreated",
            "InventoryReserved",
            "PickingTaskCreated",
            "PickingTaskStarted",
            "PickingCompleted",
            "PackingTaskCreated",
            "PackingCompleted",
            "ShipmentReady",
        ],
        "root_event": "OrderCreated",
        "lms_events": list(LMS_EVENT_TYPES),
    },
    "evening": {
        "name": "evening",
        "domain": "transportation",
        "allowed_events": [
            "CarrierAssigned",
            "VehicleAssigned",
            "ShipmentPickedUp",
            "ShipmentDelivered",
        ],
        "root_event": "ShipmentReady",
        "lms_events": list(LMS_EVENT_TYPES),
    },
    "night": {
        "name": "night",
        "domain": "outbound_transportation",
        "allowed_events": [
            "OrderCreated",
            "OrderItemCreated",
            "InventoryAllocationCreated",
            "InventoryReserved",
            "PickingTaskCreated",
            "PickingTaskStarted",
            "PickingCompleted",
            "PackingTaskCreated",
            "PackingCompleted",
            "ShipmentReady",
            "CarrierAssigned",
            "VehicleAssigned",
            "ShipmentPickedUp",
            "ShipmentDelivered",
        ],
        "root_event": "OrderCreated",
        "lms_events": list(LMS_EVENT_TYPES),
    },
}

WINDOW_BY_EVENT = {}
for window_name, contract in CONTRACT_BY_WINDOW.items():
    for event_name in contract["allowed_events"]:
        WINDOW_BY_EVENT[event_name] = window_name


def normalize_run_window(value: str | None) -> str:
    if value is None:
        return "morning"
    normalized = str(value).strip().lower()
    if normalized not in VALID_RUN_WINDOWS:
        raise ValueError(f"Unsupported run window: {value}. Expected one of {VALID_RUN_WINDOWS}")
    return normalized


def get_contract_for_window(run_window: str | None) -> dict:
    normalized = normalize_run_window(run_window)
    contract = CONTRACT_BY_WINDOW.get(normalized)
    if contract is None:
        raise ValueError(f"Missing contract for window {normalized}")
    return contract


def is_event_allowed_for_window(event_name: str | None, run_window: str | None) -> bool:
    if event_name is None:
        return False
    normalized_event = str(event_name).strip()
    if normalized_event in LMS_EVENT_TYPES:
        return True
    contract = get_contract_for_window(run_window)
    return normalized_event in set(contract["allowed_events"])


def get_root_event_for_window(run_window: str | None) -> str:
    return get_contract_for_window(run_window)["root_event"]


def validate_lms_sequence(events: Iterable[str]) -> bool:
    active_shift = False
    active_break = False

    for event_name in events:
        normalized = str(event_name).strip()
        if normalized in {"PunchIn", "ShiftStart", "WorkerCheckIn"}:
            if active_shift:
                return False
            active_shift = True
            active_break = False
            continue
        if normalized in {"PunchOut", "ShiftEnd", "WorkerCheckOut"}:
            if not active_shift:
                return False
            active_shift = False
            active_break = False
            continue
        if normalized == "BreakStart":
            if not active_shift or active_break:
                return False
            active_break = True
            continue
        if normalized == "BreakEnd":
            if not active_shift or not active_break:
                return False
            active_break = False
            continue

    return not active_break and not active_shift
