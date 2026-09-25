from .base import build_handler
from .purchase_order_created import handle as handle_purchase_order_created
from .purchase_order_approved import handle as handle_purchase_order_approved
from .supplier_shipment_created import handle as handle_supplier_shipment_created
from .domain_signal import DOMAIN_EVENT_TYPES, handle as handle_domain_signal
from .domain_persistence import (
    handle_carrier_assigned,
    handle_customer_order_created,
    handle_inventory_reserved,
    handle_outbound_shipment_created,
    handle_packing_started,
    handle_picking_task_created,
    handle_truck_departed,
    handle_worker_shift_started,
)


EVENT_HANDLER_REGISTRY = {
    "PurchaseOrderCreated": handle_purchase_order_created,
    "PurchaseOrderApproved": handle_purchase_order_approved,
    "SupplierShipmentCreated": handle_supplier_shipment_created,
    "OutboundShipmentCreated": handle_outbound_shipment_created,
    "CustomerOrderCreated": handle_customer_order_created,
    "InventoryReserved": handle_inventory_reserved,
    "WorkerShiftStarted": handle_worker_shift_started,
    "PickingTaskCreated": handle_picking_task_created,
    "PackingStarted": handle_packing_started,
    "CarrierAssigned": handle_carrier_assigned,
    "TruckDeparted": handle_truck_departed,
    "__domain_signal__": handle_domain_signal,
    "ASNReceived": build_handler("generators.purchase_order.asn_received.generate_asn_received", event_type="ASNReceived", aggregate_type="purchase_orders", aggregate_id_field="po_id"),
    "SupplierShipmentDelivered": build_handler("generators.supplier.supplier_shipment_delivered.deliver_supplier_shipment", event_type="SupplierShipmentDelivered", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ReceivingTaskCreated": build_handler("generators.warehouse.receiving_task_created_inbound.generate_receiving_task_created", event_type="ReceivingTaskCreated", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "ReceivingTaskStarted": build_handler("generators.warehouse.receiving_task_started.generate_receiving_task_started", event_type="ReceivingTaskStarted", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "GoodsReceived": build_handler("generators.warehouse.goods_received.generate_goods_received", event_type="GoodsReceived", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "StockIncreased": build_handler("generators.inventory.stock_increased.generate_stock_increased", event_type="StockIncreased", aggregate_type="inventory", aggregate_id_field="inventory_id"),
    "InventoryPutaway": build_handler("generators.inventory.inventory_putaway.generate_inventory_putaway", event_type="InventoryPutaway", aggregate_type="inventory_locations", aggregate_id_field="location_id"),
    "InventoryReceived": build_handler("generators.inventory.inventory_received.generate_inventory_received", event_type="InventoryReceived", aggregate_type="inventory", aggregate_id_field="inventory_id"),
    "InventoryReserved": handle_inventory_reserved,
    "InventoryAllocationCreated": build_handler("generators.inventory.inventory_allocation_created.generate_inventory_allocation", event_type="InventoryAllocationCreated", aggregate_type="inventory_allocations", aggregate_id_field="allocation_id"),
    "InventoryAdjusted": build_handler("generators.inventory.inventory_adjusted.generate_inventory_adjusted", event_type="InventoryAdjusted", aggregate_type="inventory_adjustments", aggregate_id_field="adjustment_id"),
    "OrderCreated": build_handler("generators.order.order_created.generate_order_created", event_type="OrderCreated", aggregate_type="orders", aggregate_id_field="order_id"),
    "OrderItemCreated": build_handler("generators.order.order_item_created.generate_order_item_created", event_type="OrderItemCreated", aggregate_type="order_items", aggregate_id_field="order_item_id"),
    "CarrierAssigned": handle_carrier_assigned,
    "DriverAssigned": build_handler("generators.transportation.driver_assigned.assign_driver", event_type="DriverAssigned", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "VehicleAssigned": build_handler("generators.transportation.vehicle_assigned.generate_vehicle_assigned", event_type="VehicleAssigned", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ShipmentReady": build_handler("generators.transportation.shipment_ready.generate_shipment_ready", event_type="ShipmentReady", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ShipmentLoaded": build_handler("generators.transportation.shipment_loaded.load_shipment", event_type="ShipmentLoaded", aggregate_type="shipment_loading_events", aggregate_id_field="loading_id"),
    "ShipmentPickedUp": build_handler("generators.transportation.shipment_picked_up.generate_shipment_picked_up", event_type="ShipmentPickedUp", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ShipmentInTransit": build_handler("generators.transportation.shipment_in_transit.generate_shipment_in_transit", event_type="ShipmentInTransit", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ShipmentArrived": build_handler("generators.transportation.shipment_arrived.generate_shipment_arrived", event_type="ShipmentArrived", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ShipmentDelivered": build_handler("generators.transportation.shipment_delivered.generate_shipment_delivered", event_type="ShipmentDelivered", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "ShipmentDispatched": build_handler("generators.transportation.shipment_dispatched.dispatch_shipment", event_type="ShipmentDispatched", aggregate_type="shipments", aggregate_id_field="shipment_id"),
    "CheckpointReached": build_handler("generators.transportation.checkpoint_reached.record_checkpoint", event_type="CheckpointReached", aggregate_type="shipment_checkpoints", aggregate_id_field="checkpoint_id"),
    "CycleCountCreated": build_handler("generators.inventory.cycle_count_created.generate_cycle_count_created", event_type="CycleCountCreated", aggregate_type="inventory_snapshots", aggregate_id_field="snapshot_id"),
    "CycleCountCompleted": build_handler("generators.inventory.cycle_count_completed.generate_cycle_count_completed", event_type="CycleCountCompleted", aggregate_type="inventory_snapshots", aggregate_id_field="snapshot_id"),
    "TaskStarted": build_handler("generators.inventory.task_started.generate_task_started", event_type="TaskStarted", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "PackingTaskCreated": build_handler("generators.warehouse.packing_task_created.generate_packing_task_created", event_type="PackingTaskCreated", aggregate_type="warehouse_tasks", aggregate_id_field="order_id"),
    "PackingTaskStarted": build_handler("generators.warehouse.packing_task_started.generate_packing_task_started", event_type="PackingTaskStarted", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "PackingCompleted": build_handler("generators.warehouse.packing_completed.generate_packing_completed", event_type="PackingCompleted", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "PickingTaskCreated": handle_picking_task_created,
    "PickingTaskStarted": build_handler("generators.warehouse.picking_task_started.generate_picking_task_started", event_type="PickingTaskStarted", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
    "PickingCompleted": build_handler("generators.warehouse.picking_completed.generate_picking_completed", event_type="PickingCompleted", aggregate_type="warehouse_tasks", aggregate_id_field="task_id"),
}

for _event_type in DOMAIN_EVENT_TYPES:
    EVENT_HANDLER_REGISTRY.setdefault(_event_type, handle_domain_signal)
