from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _field(name, data_type, nullable=False):
    return StructField(name, data_type, nullable=nullable)


def _array(*fields):
    return ArrayType(StructType(list(fields)), containsNull=False)


purchase_order_created = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("purchaseOrder", StructType([
        _field("poId", StringType()),
        _field("supplierId", StringType()),
        _field("supplierName", StringType()),
        _field("warehouseId", StringType()),
        _field("warehouseName", StringType()),
        _field("status", StringType()),
        _field("orderDate", TimestampType()),
        _field("expectedDelivery", TimestampType()),
        _field("totalItems", IntegerType()),
        _field("totalQuantity", IntegerType()),
        _field("totalAmount", DoubleType()),
        _field("currency", StringType()),
        _field("items", _array(
            _field("productId", StringType()),
            _field("productName", StringType()),
            _field("quantity", IntegerType()),
            _field("unitCost", DoubleType()),
            _field("totalCost", DoubleType()),
        )),
    ])),
])

purchase_order_approved = StructType([
    _field("eventType", StringType(), nullable=True),
    _field("occurredAt", TimestampType(), nullable=True),
    _field("purchaseOrder", StructType([
        _field("poId", StringType()),
        _field("supplierId", StringType()),
        _field("warehouseId", StringType()),
        _field("status", StringType()),
    ]), nullable=True),
    _field("event_type", StringType(), nullable=True),
    _field("po_id", StringType(), nullable=True),
    _field("supplier_id", StringType(), nullable=True),
    _field("warehouse_id", StringType(), nullable=True),
    _field("status", StringType(), nullable=True),
    _field("approved_by", StringType(), nullable=True),
    _field("order_date", TimestampType(), nullable=True),
    _field("approved_at", TimestampType(), nullable=True),
    _field("occurred_at", TimestampType(), nullable=True),
    _field("items", _array(
        _field("product_id", StringType()),
        _field("quantity", IntegerType()),
        _field("unit_cost", DoubleType()),
    ), nullable=True),
    _field("correlation_id", StringType(), nullable=True),
])

asn_received = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("asn", StructType([
        _field("shipment_id", StringType()),
        _field("po_id", StringType()),
        _field("supplier_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

supplier_shipment_created = StructType([
    _field("event_type", StringType(), nullable=True),
    _field("shipment_id", StringType(), nullable=True),
    _field("po_id", StringType(), nullable=True),
    _field("supplier_id", StringType(), nullable=True),
    _field("warehouse_id", StringType(), nullable=True),
    _field("shipment_status", StringType(), nullable=True),
    _field("shipment_date", TimestampType(), nullable=True),
    _field("occurred_at", TimestampType(), nullable=True),
    _field("expected_delivery", TimestampType(), nullable=True),
    _field("total_skus", IntegerType(), nullable=True),
    _field("total_quantity", IntegerType(), nullable=True),
    _field("items", _array(
        _field("product_id", StringType()),
        _field("quantity", IntegerType()),
    ), nullable=True),
    _field("correlation_id", StringType(), nullable=True),
    _field("eventType", StringType(), nullable=True),
    _field("occurredAt", TimestampType(), nullable=True),
    _field("shipment", StructType([
        _field("shipmentId", StringType()),
        _field("poId", StringType()),
        _field("supplierId", StringType()),
        _field("warehouseId", StringType()),
        _field("status", StringType()),
        _field("expectedDelivery", TimestampType()),
    ]), nullable=True),
])

supplier_shipment_delivered = StructType([
    _field("event_type", StringType()),
    _field("shipment_id", StringType()),
    _field("po_id", StringType()),
    _field("supplier_id", StringType()),
    _field("warehouse_id", StringType()),
    _field("status", StringType()),
    _field("delivered_at", TimestampType()),
    _field("occurred_at", TimestampType()),
    _field("correlation_id", StringType(), nullable=True),
])

inventory_received = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("shipment", StructType([
        _field("shipmentId", StringType()),
        _field("warehouseId", StringType()),
        _field("supplierId", StringType()),
        _field("items", _array(
            _field("product_id", StringType()),
            _field("received_quantity", IntegerType()),
            _field("damaged_quantity", IntegerType()),
        )),
        _field("status", StringType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

inventory_adjusted = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("inventoryAdjustment", StructType([
        _field("adjustmentId", StringType()),
        _field("taskId", StringType()),
        _field("warehouseId", StringType()),
        _field("locationId", StringType()),
        _field("productId", StringType()),
        _field("previousQuantity", IntegerType()),
        _field("adjustedQuantity", IntegerType()),
        _field("difference", IntegerType()),
        _field("reason", StringType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

inventory_reserved = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("reservation", StructType([
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("items", _array(
            _field("allocation_id", StringType()),
            _field("inventory_id", StringType()),
            _field("product_id", StringType()),
            _field("location_id", StringType(), nullable=True),
            _field("reserved_quantity", IntegerType()),
        )),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

inventory_putaway = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("inventory", StructType([
        _field("inventory_id", StringType()),
        _field("product_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("location_id", StringType()),
        _field("quantity", IntegerType()),
        _field("status", StringType()),
    ])),
    _field("putaway", StructType([
        _field("completed", BooleanType()),
        _field("location_id", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

stock_increased = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("inventory", StructType([
        _field("inventory_id", StringType()),
        _field("product_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("on_hand_quantity", IntegerType()),
        _field("available_quantity", IntegerType()),
        _field("status", StringType()),
        _field("location_id", StringType(), nullable=True),
        _field("next_event", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

inventory_allocation_created = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("allocation", StructType([
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("allocations", _array(
            _field("allocation_id", StringType()),
            _field("product_id", StringType()),
            _field("inventory_id", StringType()),
            _field("location_id", StringType(), nullable=True),
            _field("allocated_quantity", IntegerType()),
            _field("allocation_status", StringType()),
        )),
        _field("total_allocations", IntegerType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

cycle_count_created = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("cycleCountTask", StructType([
        _field("taskId", StringType()),
        _field("inventoryId", StringType()),
        _field("warehouseId", StringType()),
        _field("locationId", StringType()),
        _field("productId", StringType()),
        _field("expectedQuantity", IntegerType()),
        _field("workerId", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

cycle_count_completed = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("cycleCount", StructType([
        _field("taskId", StringType()),
        _field("warehouseId", StringType()),
        _field("locationId", StringType()),
        _field("productId", StringType()),
        _field("workerId", StringType()),
        _field("expectedQuantity", IntegerType()),
        _field("actualQuantity", IntegerType()),
        _field("discrepancy", IntegerType()),
        _field("status", StringType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

task_started = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("task", StructType([
        _field("taskId", StringType()),
        _field("taskType", StringType()),
        _field("shipmentId", StringType()),
        _field("warehouseId", StringType()),
        _field("productId", StringType()),
        _field("locationId", StringType(), nullable=True),
        _field("quantity", IntegerType()),
        _field("workerId", StringType()),
        _field("status", StringType()),
        _field("startedAt", TimestampType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

goods_received = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("goods_received", StructType([
        _field("task_id", StringType()),
        _field("shipment_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("worker_id", StringType()),
        _field("received_quantity", IntegerType()),
        _field("status", StringType()),
    ])),
    _field("inventory", StructType([
        _field("items", _array(
            _field("inventory_id", StringType()),
            _field("product_id", StringType()),
            _field("quantity", IntegerType()),
        )),
        _field("next_event", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

receiving_task_created = StructType([
    _field("eventType", StringType(), nullable=True),
    _field("occurredAt", TimestampType(), nullable=True),
    _field("event_type", StringType(), nullable=True),
    _field("occurred_at", TimestampType(), nullable=True),
    _field("task", StructType([
        _field("taskId", StringType()),
        _field("taskType", StringType()),
        _field("shipmentId", StringType()),
        _field("warehouseId", StringType()),
        _field("productId", StringType(), nullable=True),
        _field("quantity", IntegerType()),
        _field("priority", StringType(), nullable=True),
    ]), nullable=True),
    _field("receivingTask", StructType([
        _field("taskId", StringType()),
        _field("shipmentId", StringType()),
        _field("warehouseId", StringType()),
        _field("workerId", StringType()),
        _field("dockLocation", StringType()),
        _field("expectedQuantity", IntegerType()),
        _field("status", StringType()),
    ]), nullable=True),
    _field("receiving_task", StructType([
        _field("task_id", StringType()),
        _field("shipment_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("worker_id", StringType()),
        _field("dock_location", StringType()),
        _field("location_id", StringType(), nullable=True),
        _field("expected_quantity", IntegerType()),
        _field("status", StringType()),
    ]), nullable=True),
    _field("correlationId", StringType(), nullable=True),
    _field("correlation_id", StringType(), nullable=True),
])

receiving_task_started = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("receiving_task", StructType([
        _field("task_id", StringType()),
        _field("shipment_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("worker_id", StringType()),
        _field("quantity", IntegerType()),
        _field("status", StringType()),
        _field("started_at", TimestampType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

picking_task_created = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("picking", StructType([
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("tasks", _array(
            _field("task_id", StringType()),
            _field("allocation_id", StringType()),
            _field("product_id", StringType()),
            _field("location_id", StringType(), nullable=True),
            _field("quantity", IntegerType()),
            _field("status", StringType()),
        )),
        _field("task_count", IntegerType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

picking_task_started = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("pickingTask", StructType([
        _field("taskId", StringType()),
        _field("warehouseId", StringType()),
        _field("workerId", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

picking_completed = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("picking", StructType([
        _field("task_id", StringType()),
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("worker_id", StringType()),
        _field("product_id", StringType()),
        _field("quantity", IntegerType()),
        _field("inventory_id", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

packing_task_created = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("packing_task", StructType([
        _field("task_id", StringType()),
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("quantity", IntegerType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

packing_task_started = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("packingTask", StructType([
        _field("taskId", StringType()),
        _field("warehouseId", StringType()),
        _field("workerId", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

packing_completed = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("packing", StructType([
        _field("task_id", StringType()),
        _field("package_id", StringType()),
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("worker_id", StringType()),
        _field("quantity", IntegerType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

order_created = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("order", StructType([
        _field("order_id", StringType()),
        _field("customer_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("status", StringType()),
        _field("channel", StringType()),
        _field("priority", StringType()),
        _field("items_created", BooleanType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

order_item_created = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("order", StructType([
        _field("order_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("items", _array(
            _field("product_id", StringType()),
            _field("inventory_id", StringType()),
            _field("location_id", StringType(), nullable=True),
            _field("quantity", IntegerType()),
            _field("unit_price", DoubleType()),
            _field("total_price", DoubleType()),
        )),
        _field("total_items", IntegerType()),
        _field("total_quantity", IntegerType()),
        _field("total_amount", DoubleType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

shipment_loaded = StructType([
    _field("shipment_id", StringType()),
    _field("vehicle_id", StringType()),
    _field("trailer_id", StringType(), nullable=True),
    _field("warehouse_id", StringType()),
    _field("loaded_quantity", IntegerType()),
    _field("loaded_at", TimestampType()),
])

shipment_dispatched = StructType([
    _field("shipment_id", StringType()),
    _field("vehicle_id", StringType()),
    _field("driver_id", StringType()),
    _field("trailer_id", StringType(), nullable=True),
    _field("tracking_id", StringType()),
    _field("status", StringType()),
    _field("departure_time", TimestampType()),
    _field("estimated_arrival", TimestampType()),
])

shipment_arrived = StructType([
    _field("event_type", StringType()),
    _field("shipment_id", StringType()),
    _field("po_id", StringType()),
    _field("supplier_id", StringType()),
    _field("warehouse_id", StringType()),
    _field("tracking_id", StringType()),
    _field("vehicle_id", StringType()),
    _field("driver_id", StringType()),
    _field("quantity", IntegerType()),
    _field("arrival_time", TimestampType()),
    _field("correlation_id", StringType(), nullable=True),
])

shipment_delivered = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("shipment", StructType([
        _field("shipment_id", StringType()),
        _field("package_id", StringType()),
        _field("order_id", StringType()),
        _field("fulfillment_id", StringType()),
        _field("vehicle_id", StringType()),
        _field("trailer_id", StringType(), nullable=True),
        _field("driver_id", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

vehicle_assigned = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("vehicle_assignment", StructType([
        _field("shipment_id", StringType()),
        _field("vehicle_id", StringType()),
        _field("trailer_id", StringType(), nullable=True),
        _field("driver_id", StringType()),
        _field("tracking_id", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

carrier_assigned = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("transportation", StructType([
        _field("shipment_id", StringType()),
        _field("vehicle_id", StringType()),
        _field("trailer_id", StringType(), nullable=True),
        _field("driver_id", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

shipment_picked_up = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("shipment", StructType([
        _field("shipment_id", StringType()),
        _field("order_id", StringType()),
        _field("vehicle_id", StringType()),
        _field("trailer_id", StringType(), nullable=True),
        _field("driver_id", StringType()),
        _field("tracking_id", StringType()),
        _field("status", StringType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

shipment_in_transit = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("shipment", StructType([
        _field("shipmentId", StringType()),
        _field("fulfillmentId", StringType()),
        _field("orderId", StringType()),
        _field("packageId", StringType()),
        _field("warehouseId", StringType()),
        _field("vehicleId", StringType()),
        _field("trailerId", StringType(), nullable=True),
        _field("driverId", StringType()),
        _field("status", StringType()),
        _field("pickedUpAt", TimestampType()),
        _field("inTransitAt", TimestampType()),
        _field("expectedDelivery", TimestampType(), nullable=True),
        _field("estimatedArrival", TimestampType()),
        _field("trackingId", StringType()),
        _field("latitude", DoubleType()),
        _field("longitude", DoubleType()),
    ])),
    _field("transportation", StructType([
        _field("status", StringType()),
        _field("vehicleStatus", StringType()),
        _field("trailerStatus", StringType()),
        _field("driverStatus", StringType()),
    ])),
    _field("tracking", StructType([
        _field("trackingId", StringType()),
        _field("status", StringType()),
        _field("latitude", DoubleType()),
        _field("longitude", DoubleType()),
        _field("departureTime", TimestampType()),
        _field("estimatedArrival", TimestampType()),
    ])),
    _field("correlationId", StringType(), nullable=True),
])

driver_assigned = StructType([
    _field("event_type", StringType()),
    _field("shipment_id", StringType()),
    _field("vehicle_id", StringType()),
    _field("vehicle_number", StringType()),
    _field("trailer_id", StringType(), nullable=True),
    _field("trailer_number", StringType(), nullable=True),
    _field("driver_id", StringType()),
    _field("driver_name", StringType()),
    _field("assigned_at", TimestampType()),
])

shipment_ready = StructType([
    _field("event_type", StringType()),
    _field("occurred_at", TimestampType()),
    _field("shipment", StructType([
        _field("shipment_id", StringType()),
        _field("package_id", StringType()),
        _field("order_id", StringType()),
        _field("fulfillment_id", StringType()),
        _field("warehouse_id", StringType()),
        _field("status", StringType()),
        _field("expected_delivery", TimestampType()),
    ])),
    _field("correlation_id", StringType(), nullable=True),
])

checkpoint_reached = StructType([
    _field("checkpoint_id", StringType()),
    _field("shipment_id", StringType()),
    _field("tracking_id", StringType()),
    _field("vehicle_id", StringType()),
    _field("driver_id", StringType()),
    _field("checkpoint_sequence", IntegerType()),
    _field("location", StringType()),
    _field("latitude", DoubleType()),
    _field("longitude", DoubleType()),
    _field("distance_completed_km", DoubleType()),
    _field("traffic_status", StringType()),
    _field("weather_condition", StringType()),
    _field("delay_minutes", IntegerType()),
])

worker_assigned = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("task", StructType([
        _field("taskId", StringType()),
        _field("taskType", StringType()),
        _field("warehouseId", StringType()),
    ])),
    _field("worker", StructType([
        _field("workerId", StringType()),
        _field("role", StringType()),
    ])),
])


event_schemas = {
    "PurchaseOrderCreated": purchase_order_created,
    "PurchaseOrderApproved": purchase_order_approved,
    "ASNReceived": asn_received,
    "SupplierShipmentCreated": supplier_shipment_created,
    "SupplierShipmentDelivered": supplier_shipment_delivered,
    "InventoryReceived": inventory_received,
    "InventoryAdjusted": inventory_adjusted,
    "InventoryReserved": inventory_reserved,
    "InventoryPutaway": inventory_putaway,
    "InventoryAllocationCreated": inventory_allocation_created,
    "StockIncreased": stock_increased,
    "CycleCountCreated": cycle_count_created,
    "CycleCountCompleted": cycle_count_completed,
    "TaskStarted": task_started,
    "GoodsReceived": goods_received,
    "ReceivingTaskCreated": receiving_task_created,
    "ReceivingTaskStarted": receiving_task_started,
    "PickingTaskCreated": picking_task_created,
    "PickingTaskStarted": picking_task_started,
    "PickingCompleted": picking_completed,
    "PackingTaskCreated": packing_task_created,
    "PackingTaskStarted": packing_task_started,
    "PackingCompleted": packing_completed,
    "OrderCreated": order_created,
    "OrderItemCreated": order_item_created,
    "ShipmentLoaded": shipment_loaded,
    "ShipmentDispatched": shipment_dispatched,
    "ShipmentArrived": shipment_arrived,
    "ShipmentDelivered": shipment_delivered,
    "VehicleAssigned": vehicle_assigned,
    "CarrierAssigned": carrier_assigned,
    "ShipmentPickedUp": shipment_picked_up,
    "ShipmentInTransit": shipment_in_transit,
    "DriverAssigned": driver_assigned,
    "ShipmentReady": shipment_ready,
    "CheckpointReached": checkpoint_reached,
    "WorkerAssigned": worker_assigned,
}

def get_spark_schema(*,event_name):
    if event_name == "":
        return "Invalid parameter"

    key_list = list(event_schemas.keys())
    if str(event_name).strip() not in key_list:
        return "No such event"

    return event_schemas[str(event_name).strip()]


# Schema for the generic inventory payload helper.
inventory_event = StructType([
    _field("eventType", StringType()),
    _field("occurredAt", TimestampType()),
    _field("inventory", StructType([
        _field("productId", StringType()),
        _field("warehouseId", StringType()),
        _field("availableQuantity", IntegerType()),
    ])),
])
