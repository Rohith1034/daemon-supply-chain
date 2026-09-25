import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR_DIR = ROOT / "simulator"
sys.path.insert(0, str(SIMULATOR_DIR))

from core.event_scheduler.event_queue import EventQueue
from core.db import Database
from generators.purchase_order.purchase_order_created import generate_purchase_order
from master_simulator import run_simulation_cycle
from core.multi_domain import seed_multi_domain_events
from core.event_scheduler.event_registry import EVENT_REGISTRY


def apply_runtime_migration():
    migration_path = ROOT / "migrations" / "001_event_execution.sql"
    with Database() as db:
        db.cursor.execute(migration_path.read_text(encoding="utf-8"))


def main():
    apply_runtime_migration()
    generate_purchase_order()
    queue = EventQueue(memory_mode=False)
    start_time = datetime.now(timezone.utc)
    seeded = seed_multi_domain_events(queue=queue, start_time=start_time)
    batches = []
    current_time = start_time
    for _ in range(100):
        batches.append(run_simulation_cycle(simulation_time=current_time, queue=queue, registry=EVENT_REGISTRY))
        with Database() as db:
            next_event = db.fetch_one(
                """
                SELECT COALESCE(
                    (payload->>'next_retry_time')::timestamptz,
                    (payload->>'event_scheduled_time')::timestamptz
                ) AS scheduled_time
                FROM event_outbox
                WHERE status IN ('PENDING', 'RETRY_SCHEDULED')
                ORDER BY scheduled_time
                LIMIT 1
                """
            )
        if not next_event:
            break
        current_time = max(current_time, next_event["scheduled_time"])
    print(json.dumps({"seeded_events": seeded, "batches": batches, "final_queue_size": queue.queue_size}, default=str, indent=2))


if __name__ == "__main__":
    main()