from .base import build_handler


handle = build_handler(
    "generators.transportation.shipment_in_transit.mark_shipment_in_transit",
    event_type="ShipmentInTransit",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
