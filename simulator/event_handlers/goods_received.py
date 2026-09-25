from .base import build_handler


handle = build_handler(
    "generators.warehouse.goods_received.generate_goods_received",
    event_type="GoodsReceived",
    aggregate_type="warehouse_tasks",
    aggregate_id_field="task_id",
)
