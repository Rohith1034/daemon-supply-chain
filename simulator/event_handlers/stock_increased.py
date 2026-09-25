from .base import build_handler


handle = build_handler(
    "generators.inventory.stock_increased.generate_stock_increased",
    event_type="StockIncreased",
    aggregate_type="inventory",
    aggregate_id_field="inventory_id",
)
