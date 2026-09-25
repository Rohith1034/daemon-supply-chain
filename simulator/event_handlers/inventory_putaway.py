from .base import build_handler


handle = build_handler(
    "generators.inventory.inventory_putaway.generate_inventory_putaway",
    event_type="InventoryPutaway",
    aggregate_type="inventory_locations",
    aggregate_id_field="location_id",
)
