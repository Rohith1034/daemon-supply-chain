from .base import build_handler


handle = build_handler(
    "generators.transportation.carrier_assigned.assign_carrier",
    event_type="CarrierAssigned",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
