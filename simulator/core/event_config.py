import random
from datetime import timedelta

PURCHASE_ORDER_APPROVAL_MIN_HOURS = 2
PURCHASE_ORDER_APPROVAL_MAX_HOURS = 6

SHIPMENT_CREATED_MIN_HOURS = 2
SHIPMENT_CREATED_MAX_HOURS = 24

ASN_MIN_HOURS = 1
ASN_MAX_HOURS = 5

DELIVERY_MIN_HOURS = 1
DELIVERY_MAX_HOURS = 24

RECEIVING_TASK_MIN_MINUTES = 10
RECEIVING_TASK_MAX_MINUTES = 120

GOODS_RECEIVED_MIN_MINUTES = 15
GOODS_RECEIVED_MAX_MINUTES = 90

EVENT_DELAY_WINDOWS = {
    "PurchaseOrderCreated": {"min_hours": 0, "max_hours": 0},
    "PurchaseOrderApproved": {"min_hours": PURCHASE_ORDER_APPROVAL_MIN_HOURS, "max_hours": PURCHASE_ORDER_APPROVAL_MAX_HOURS},
    "SupplierShipmentCreated": {"min_hours": SHIPMENT_CREATED_MIN_HOURS, "max_hours": SHIPMENT_CREATED_MAX_HOURS},
    "ASNReceived": {"min_hours": ASN_MIN_HOURS, "max_hours": ASN_MAX_HOURS},
    "SupplierShipmentDelivered": {"min_hours": DELIVERY_MIN_HOURS, "max_hours": DELIVERY_MAX_HOURS},
    "ReceivingTaskCreated": {"min_hours": 0, "max_hours": 2},
    "ReceivingTaskStarted": {"min_hours": 0, "max_hours": 1},
    "GoodsReceived": {"min_hours": 0, "max_hours": 2},
    "StockIncreased": {"min_hours": 0, "max_hours": 1},
    "InventoryPutaway": {"min_hours": 0, "max_hours": 1},
    "OrderCreated": {"min_hours": 0, "max_hours": 0},
    "OrderItemCreated": {"min_hours": 0, "max_hours": 1},
    "InventoryAllocationCreated": {"min_hours": 0, "max_hours": 2},
    "InventoryReserved": {"min_hours": 0, "max_hours": 2},
    "PickingTaskCreated": {"min_hours": 0, "max_hours": 1},
    "ShipmentReady": {"min_hours": 0, "max_hours": 1},
    "CarrierAssigned": {"min_hours": 0, "max_hours": 1},
    "ShipmentPickedUp": {"min_hours": 0, "max_hours": 2},
    "ShipmentInTransit": {"min_hours": 2, "max_hours": 12},
    "ShipmentDelivered": {"min_hours": 1, "max_hours": 24},
}


def get_event_delay(event_type, *, base_time=None):
    window = EVENT_DELAY_WINDOWS.get(event_type, {"min_hours": 0, "max_hours": 1})
    delay_hours = random.randint(window["min_hours"], window["max_hours"])
    return timedelta(hours=delay_hours)


def schedule_event_time(base_time, event_type):
    delay = get_event_delay(event_type)
    return base_time + delay
