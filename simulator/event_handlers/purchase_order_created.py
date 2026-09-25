from .base import build_handler


handle = build_handler(
    "generators.purchase_order.purchase_order_created.generate_purchase_order",
    event_type="PurchaseOrderCreated",
    aggregate_type="purchase_orders",
    aggregate_id_field="po_id",
)
