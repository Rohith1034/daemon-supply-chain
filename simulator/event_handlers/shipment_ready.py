from .base import build_handler


handle = build_handler(
    "generators.transportation.shipment_ready.prepare_shipment_ready",
    event_type="ShipmentReady",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
