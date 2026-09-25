import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.db import Database

TABLES = [
    "event_execution_log",
    "event_execution",
    "event_outbox",
    "inventory_transactions",
    "inventory_allocations",
    "inventory_reservations",
    "inventory",
    "inventory_locations",
    "inventory_snapshots",
    "shipment_items",
    "shipments",
    "shipment_transportation",
    "warehouse_tasks",
    "purchase_order_items",
    "purchase_orders",
    "order_items",
    "orders",
]


def reset_transaction_tables() -> None:
    with Database() as db:
        migration_path = ROOT.parent / "migrations" / "001_event_execution.sql"
        db.cursor.execute(migration_path.read_text(encoding="utf-8"))
        table_list = ", ".join(TABLES)
        db.execute(
            f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE;"
        )
        db.commit()
        print(f"Reset transactional tables: {table_list}")


if __name__ == "__main__":
    reset_transaction_tables()
