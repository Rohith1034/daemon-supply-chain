import sys
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent
SIM = ROOT / 'simulator'
sys.path.insert(0, str(SIM))

from core.db import Database

TABLES = [
    'event_outbox',
    'event_execution_log',
    'inventory_transactions',
    'inventory_allocations',
    'inventory_reservations',
    'inventory',
    'inventory_locations',
    'inventory_snapshots',
    'shipment_items',
    'shipments',
    'shipment_transportation',
    'warehouse_tasks',
    'purchase_order_items',
    'purchase_orders',
    'order_items',
    'orders',
]


def reset_db():
    with Database() as db:
        db.execute(f"TRUNCATE TABLE {', '.join(TABLES)} RESTART IDENTITY CASCADE;")


for run_no in range(1, 4):
    reset_db()
    print(f'\n=== RUN {run_no} START ===')
    runpy.run_module('scripts.run_db_simulation', run_name='__main__')
    with Database() as db:
        duplicate_query = """
        SELECT
            correlation_id,
            event_type,
            COUNT(DISTINCT aggregate_id) AS distinct_aggregates,
            STRING_AGG(DISTINCT aggregate_id, ', ' ORDER BY aggregate_id) AS aggregate_ids
        FROM event_outbox
        WHERE event_type IN (
            'SupplierShipmentCreated',
            'ASNReceived',
            'SupplierShipmentDelivered',
            'ReceivingTaskCreated',
            'ReceivingTaskStarted',
            'GoodsReceived',
            'StockIncreased'
        )
        GROUP BY correlation_id, event_type
        ORDER BY correlation_id, event_type;
        """
        sim_root = db.fetch_one("SELECT COUNT(*) AS c FROM event_outbox WHERE aggregate_id = 'SIM-ROOT';")
        status_counts = db.fetch_all("SELECT status, COUNT(*) AS c FROM event_outbox GROUP BY status ORDER BY status;")
        business_table_counts = db.fetch_all("""
            SELECT 'purchase_orders' AS table_name, COUNT(*) AS c FROM purchase_orders
            UNION ALL SELECT 'orders', COUNT(*) FROM orders
            UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
            UNION ALL SELECT 'shipments', COUNT(*) FROM shipments
            UNION ALL SELECT 'warehouse_tasks', COUNT(*) FROM warehouse_tasks
            UNION ALL SELECT 'inventory', COUNT(*) FROM inventory
            UNION ALL SELECT 'inventory_transactions', COUNT(*) FROM inventory_transactions
            UNION ALL SELECT 'inventory_allocations', COUNT(*) FROM inventory_allocations
            UNION ALL SELECT 'shipment_transportation', COUNT(*) FROM shipment_transportation;
        """)
        execution_status = db.fetch_all("SELECT status, COUNT(*) AS c FROM event_execution_log GROUP BY status ORDER BY status;")
        print('Duplicate lineage rows:')
        for row in db.fetch_all(duplicate_query):
            print(dict(row))
        print('SIM-ROOT rows:', dict(sim_root))
        print('event_outbox status counts:', [dict(r) for r in status_counts])
        print('business table counts:', [dict(r) for r in business_table_counts])
        print('execution log status counts:', [dict(r) for r in execution_status])
