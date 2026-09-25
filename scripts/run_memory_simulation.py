import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR_DIR = ROOT / "simulator"
sys.path.insert(0, str(SIMULATOR_DIR))

from core.event_scheduler.event_queue import EventQueue
from master_simulator import run_simulation_cycle


def main():
    queue = EventQueue(memory_mode=True)
    now = datetime.now(timezone.utc)
    queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-MEMORY-001",
        correlation_id="memory-run",
        payload={"po_id": "PO-MEMORY-001", "entity_type": "PurchaseOrder", "current_state": "CREATED", "next_state": "APPROVED", "priority": "HIGH"},
        scheduled_time=now,
        priority="HIGH",
    )
    result = run_simulation_cycle(simulation_time=now, queue=queue, checkpoint_path=None, registry={
        "PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []},
    })
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()