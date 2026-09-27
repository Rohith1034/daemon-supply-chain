import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from psycopg2 import sql

from core.db import Database

MASTER_TABLES = {
    "customers",
    "drivers",
    "products",
    "suppliers",
    "trailers",
    "vehicles",
    "warehouse_locations",
    "warehouses",
    "workers",
}

PRESERVED_REFERENCE_TABLES = {"product_supplier_mapping"}

TRANSACTION_TABLES = {
    "delivery_confirmation",
    "event_execution_log",
    "event_execution",
    "event_outbox",
    "inventory",
    "inventory_adjustments",
    "inventory_allocations",
    "inventory_locations",
    "inventory_reservations",
    "inventory_snapshots",
    "inventory_transactions",
    "order_items",
    "orders",
    "outbound_fulfillment",
    "outbound_shipment_loading_events",
    "outbound_shipment_tracking",
    "outbound_shipment_transportation",
    "outbound_shipments",
    "package_items",
    "packages",
    "payments",
    "picking_items",
    "purchase_order_items",
    "purchase_orders",
    "shipment_checkpoints",
    "shipment_items",
    "shipment_loading_events",
    "shipment_tracking",
    "shipment_transportation",
    "shipments",
    "warehouse_tasks",
    "worker_attendance",
    "worker_productivity",
}

PRESERVED_TABLES = MASTER_TABLES | PRESERVED_REFERENCE_TABLES
EXPECTED_PUBLIC_TABLES = PRESERVED_TABLES | TRANSACTION_TABLES


def _table_counts(db, tables):
    counts = {}
    for table in sorted(tables):
        db.cursor.execute(
            sql.SQL("SELECT COUNT(*) AS row_count FROM {}").format(sql.Identifier(table))
        )
        counts[table] = db.cursor.fetchone()["row_count"]
    return counts


def reset_transaction_tables() -> dict:
    with Database() as db:
        rows = db.fetch_all(
            """SELECT table_name
               FROM information_schema.tables
               WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"""
        )
        actual_tables = {row["table_name"] for row in rows}
        if actual_tables != EXPECTED_PUBLIC_TABLES:
            missing = sorted(EXPECTED_PUBLIC_TABLES - actual_tables)
            unexpected = sorted(actual_tables - EXPECTED_PUBLIC_TABLES)
            raise RuntimeError(
                f"Public schema differs from the reviewed reset inventory; "
                f"missing={missing}, unexpected={unexpected}"
            )

        master_counts_before = _table_counts(db, MASTER_TABLES)
        reference_counts_before = _table_counts(db, PRESERVED_REFERENCE_TABLES)
        db.cursor.execute(
            sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                sql.SQL(", ").join(
                    sql.Identifier(table) for table in sorted(TRANSACTION_TABLES)
                )
            )
        )

        remaining_counts = _table_counts(db, TRANSACTION_TABLES)
        if any(remaining_counts.values()):
            raise RuntimeError(f"Transactional reset left rows behind: {remaining_counts}")

        master_counts_after = _table_counts(db, MASTER_TABLES)
        reference_counts_after = _table_counts(db, PRESERVED_REFERENCE_TABLES)
        if master_counts_after != master_counts_before:
            raise RuntimeError(
                f"Required master counts changed during reset: "
                f"before={master_counts_before}, after={master_counts_after}"
            )
        if reference_counts_after != reference_counts_before:
            raise RuntimeError(
                f"Preserved reference counts changed during reset: "
                f"before={reference_counts_before}, after={reference_counts_after}"
            )

        report = {
            "transaction_tables_cleared": sorted(TRANSACTION_TABLES),
            "master_counts": master_counts_after,
            "reference_counts": reference_counts_after,
            "transaction_counts_after": remaining_counts,
        }
        print(f"Reset {len(TRANSACTION_TABLES)} transactional tables")
        print(f"Required master counts unchanged: {master_counts_after}")
        print(f"Preserved reference table counts unchanged: {reference_counts_after}")
        return report


if __name__ == "__main__":
    reset_transaction_tables()
