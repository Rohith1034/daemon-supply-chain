from .base import build_handler


handle = build_handler(
    "generators.purchase_order.purchase_order_approved.approve_purchase_order",
    event_type="PurchaseOrderApproved",
    aggregate_type="purchase_orders",
    aggregate_id_field="po_id",
)
