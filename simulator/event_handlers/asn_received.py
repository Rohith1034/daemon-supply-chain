from .base import build_handler


handle = build_handler(
    "generators.purchase_order.asn_received.generate_asn_received",
    event_type="ASNReceived",
    aggregate_type="shipments",
    aggregate_id_field="shipment_id",
)
