from __future__ import annotations

STATE_MACHINE = {
    "PurchaseOrder": {
        "CREATED": {"APPROVED", "CANCELLED"},
        "APPROVED": {"CANCELLED"},
        "CANCELLED": set(),
    },
    "Shipment": {
        "PENDING": {"READY"},
        "CREATED": {"ASN_RECEIVED", "READY"},
        "READY": {"ASSIGNED", "IN_TRANSIT"},
        "ASN_RECEIVED": {"IN_TRANSIT"},
        "ASSIGNED": {"LOADED"},
        "LOADED": {"IN_TRANSIT"},
        "IN_TRANSIT": {"DELIVERED"},
        "DELIVERED": set(),
    },
    "Inventory": {
        "RECEIVED": {"AVAILABLE"},
        "AVAILABLE": {"PUTAWAY"},
        "READY": {"PUTAWAY"},
        "PUTAWAY": set(),
    },
    "WarehouseTask": {
        "CREATED": {"STARTED", "PICKING", "PACKING"},
        "STARTED": {"COMPLETED"},
        "PICKING": {"IN_PROGRESS"},
        "IN_PROGRESS": {"COMPLETED"},
        "PACKING": {"COMPLETED"},
        "COMPLETED": set(),
    },
    "Order": {
        "CREATED": {"ALLOCATED", "RESERVED"},
        "PENDING": {"ALLOCATED"},
        "ALLOCATED": {"RESERVED"},
        "RESERVED": {"PICKING"},
        "PICKING": {"PACKED"},
        "PACKED": {"SHIPPED"},
        "SHIPPED": {"DELIVERED"},
        "DELIVERED": set(),
    },
}

EVENT_TO_ENTITY_AND_STATE = {
    "PurchaseOrderCreated": ("PurchaseOrder", "CREATED"),
    "PurchaseOrderApproved": ("PurchaseOrder", "APPROVED"),
    "PurchaseOrderCancelled": ("PurchaseOrder", "CANCELLED"),
    "SupplierShipmentCreated": ("Shipment", "CREATED"),
    "ASNReceived": ("Shipment", "ASN_RECEIVED"),
    "ShipmentReady": ("Shipment", "READY"),
    "CarrierAssigned": ("Shipment", "ASSIGNED"),
    "VehicleAssigned": ("Shipment", "LOADED"),
    "ShipmentPickedUp": ("Shipment", "IN_TRANSIT"),
    "ShipmentInTransit": ("Shipment", "IN_TRANSIT"),
    "ShipmentDelivered": ("Shipment", "DELIVERED"),
    "ReceivingTaskCreated": ("WarehouseTask", "CREATED"),
    "ReceivingTaskStarted": ("WarehouseTask", "STARTED"),
    "GoodsReceived": ("WarehouseTask", "COMPLETED"),
    "StockIncreased": ("Inventory", "AVAILABLE"),
    "InventoryPutaway": ("Inventory", "PUTAWAY"),
    "OrderCreated": ("Order", "CREATED"),
    "OrderItemCreated": ("Order", "ALLOCATED"),
    "InventoryAllocationCreated": ("Order", "ALLOCATED"),
    "InventoryReserved": ("Order", "RESERVED"),
    "PickingTaskCreated": ("WarehouseTask", "CREATED"),
    "PickingTaskStarted": ("WarehouseTask", "PICKING"),
    "PickingCompleted": ("WarehouseTask", "COMPLETED"),
    "PackingTaskCreated": ("WarehouseTask", "CREATED"),
    "PackingCompleted": ("WarehouseTask", "COMPLETED"),
    "ShipmentDispatched": ("Order", "SHIPPED"),
    "OrderDelivered": ("Order", "DELIVERED"),
}


def normalize_entity_type(value):
    if value is None:
        return None

    normalized = str(value).strip().lower()
    aliases = {
        "purchase_orders": "PurchaseOrder",
        "purchase_order": "PurchaseOrder",
        "shipments": "Shipment",
        "shipment": "Shipment",
        "inventory": "Inventory",
        "inventory_locations": "Inventory",
        "inventory_location": "Inventory",
        "orders": "Order",
        "order": "Order",
        "warehouse_tasks": "WarehouseTask",
        "warehouse_task": "WarehouseTask",
        "inventory_allocations": "Order",
        "inventory_reservations": "Order",
        "order_items": "Order",
    }
    return aliases.get(normalized, value)


def validate_transition(current_state, next_state, entity_type=None):
    if current_state is None or next_state is None:
        return True

    current = str(current_state).upper()
    next = str(next_state).upper()
    entity = normalize_entity_type(entity_type)

    if entity is None:
        for key, states in STATE_MACHINE.items():
            if current in states and next in states.get(current, set()):
                return True
        return False

    states = STATE_MACHINE.get(entity, {})
    return next in states.get(current, set())


def infer_next_state(event_type):
    return EVENT_TO_ENTITY_AND_STATE.get(event_type, (None, None))[1]


def infer_entity_type(event_type):
    return EVENT_TO_ENTITY_AND_STATE.get(event_type, (None, None))[0]
