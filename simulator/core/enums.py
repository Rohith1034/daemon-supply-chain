from enum import Enum



# ======================================
# Purchase Order Status
# ======================================

class POStatus(str, Enum):

    CREATED = "CREATED"

    APPROVED = "APPROVED"

    CANCELLED = "CANCELLED"



# ======================================
# Shipment Status
# ======================================

class ShipmentStatus(str, Enum):

    CREATED = "CREATED"

    LOADED = "LOADED"

    DISPATCHED = "DISPATCHED"

    IN_TRANSIT = "IN_TRANSIT"

    DELAYED = "DELAYED"

    ARRIVED = "ARRIVED"

    RECEIVING = "RECEIVING"

    RECEIVED = "RECEIVED"

    DELIVERED = "DELIVERED"



# ======================================
# Order Status
# ======================================

class OrderStatus(str, Enum):

    CREATED = "CREATED"

    PAYMENT_PENDING = "PAYMENT_PENDING"

    CONFIRMED = "CONFIRMED"

    PICKING = "PICKING"

    PACKED = "PACKED"

    READY_TO_SHIP = "READY_TO_SHIP"

    SHIPPED = "SHIPPED"

    DELIVERED = "DELIVERED"

    CANCELLED = "CANCELLED"



# ======================================
# Payment Status
# ======================================

class PaymentStatus(str, Enum):

    SUCCESS = "SUCCESS"

    FAILED = "FAILED"

    REFUNDED = "REFUNDED"



# ======================================
# Inventory Status
# ======================================

class InventoryStatus(str, Enum):

    AVAILABLE = "AVAILABLE"

    RESERVED = "RESERVED"

    DAMAGED = "DAMAGED"

    EXPIRED = "EXPIRED"



# ======================================
# Warehouse Task Status
# ======================================

class TaskStatus(str, Enum):

    CREATED = "CREATED"

    ASSIGNED = "ASSIGNED"

    STARTED = "STARTED"

    COMPLETED = "COMPLETED"

    FAILED = "FAILED"



# ======================================
# Warehouse Task Types
# ======================================

class TaskType(str, Enum):

    RECEIVING="RECEIVING"

    PUTAWAY="PUTAWAY"

    PICKING="PICKING"

    PACKING="PACKING"

    CYCLE_COUNT="CYCLE_COUNT"

    LOADING="LOADING"

    UNLOADING="UNLOADING"

    QUALITY_CHECK="QUALITY_CHECK"

    INVENTORY_TRANSFER="INVENTORY_TRANSFER"

    REPLENISHMENT="REPLENISHMENT"

    RETURNS_PROCESSING="RETURNS_PROCESSING"



# ======================================
# Vehicle Status
# ======================================

class VehicleStatus(str, Enum):

    AVAILABLE = "AVAILABLE"

    ASSIGNED = "ASSIGNED"

    IN_TRANSIT = "IN_TRANSIT"

    MAINTENANCE = "MAINTENANCE"



# ======================================
# Driver Status
# ======================================

class DriverStatus(str, Enum):

    ACTIVE = "ACTIVE"

    ON_TRIP = "ON_TRIP"

    OFF_DUTY = "OFF_DUTY"



# ======================================
# Trailer Status
# ======================================

class TrailerStatus(str, Enum):

    AVAILABLE = "AVAILABLE"

    ASSIGNED = "ASSIGNED"

    LOADED = "LOADED"

    MAINTENANCE = "MAINTENANCE"


class InventoryTransactionType(str, Enum):

    STOCK_RECEIVED="STOCK_RECEIVED"

    PUTAWAY_COMPLETED="PUTAWAY_COMPLETED"

    STOCK_PICKED="STOCK_PICKED"

    STOCK_RESERVED="STOCK_RESERVED"

    STOCK_RELEASED="STOCK_RELEASED"

    STOCK_ADJUSTED="STOCK_ADJUSTED"

class WorkerStatus(str, Enum):

    AVAILABLE="AVAILABLE"

    BUSY="BUSY"

    BREAK = "BREAK"

    OFF_SHIFT="OFF_SHIFT"


class ShiftStatus(str, Enum):

    ACTIVE="ACTIVE"

    COMPLETED="COMPLETED"



class TaskEventType(str, Enum):

    CREATED="CREATED"

    ASSIGNED="ASSIGNED"

    STARTED="STARTED"

    COMPLETED="COMPLETED"


class TaskEventStatus(str, Enum):

    CREATED="CREATED"

    ASSIGNED="ASSIGNED"

    STARTED="STARTED"

    COMPLETED="COMPLETED"

    FAILED="FAILED"


class WarehouseTaskEvent(str, Enum):

    RECEIVING_CREATED="RECEIVING_CREATED"

    PUTAWAY_CREATED="PUTAWAY_CREATED"

    PICKING_CREATED="PICKING_CREATED"

    PACKING_CREATED="PACKING_CREATED"

    TASK_STARTED="TASK_STARTED"

    TASK_COMPLETED="TASK_COMPLETED"

    QUALITY_CHECK_COMPLETED="QUALITY_CHECK_COMPLETED"

class FulfillmentStatus(str, Enum):

    CREATED="CREATED"

    PROCESSING="PROCESSING"

    COMPLETED="COMPLETED"

    CANCELLED="CANCELLED"



class DeliveryStatus(str, Enum):

    CREATED="CREATED"

    OUT_FOR_DELIVERY="OUT_FOR_DELIVERY"

    DELIVERED="DELIVERED"

    FAILED="FAILED"



class OutboundEventType(str, Enum):

    FULFILLMENT_STARTED="FULFILLMENT_STARTED"

    SHIPMENT_CREATED="SHIPMENT_CREATED"

    ORDER_SHIPPED="ORDER_SHIPPED"

    DELIVERY_CONFIRMED="DELIVERY_CONFIRMED"

    RETURN_CREATED="RETURN_CREATED"