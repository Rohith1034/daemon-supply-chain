from .base import build_handler


handle = build_handler(
    "generators.transportation.shipment_picked_up.pick_up_shipment",
    event_type="ShipmentPickedUp",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
