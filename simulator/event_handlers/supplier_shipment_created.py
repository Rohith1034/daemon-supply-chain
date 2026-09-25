from .base import build_handler


handle = build_handler(
    "generators.supplier.supplier_shipment_created.create_supplier_shipment",
    event_type="SupplierShipmentCreated",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
