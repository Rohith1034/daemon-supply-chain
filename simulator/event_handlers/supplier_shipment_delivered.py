from .base import build_handler


handle = build_handler(
    "generators.supplier.supplier_shipment_delivered.deliver_supplier_shipment",
    event_type="SupplierShipmentDelivered",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
