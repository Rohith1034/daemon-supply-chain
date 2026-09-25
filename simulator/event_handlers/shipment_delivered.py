from .base import build_handler


handle = build_handler(
    "generators.transportation.shipment_delivered.complete_shipment",
    event_type="ShipmentDelivered",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
