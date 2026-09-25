from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.event_scheduler.event_queue import EventQueue
from master_simulator import run_simulation_cycle


OUTPUT_PATH = ROOT / "output" / "full_event_flow_response.json"


def make_registry():
    def create_event(event_type, aggregate_type, aggregate_id, correlation_id, payload, scheduled_minutes):
        return {
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "correlation_id": correlation_id,
            "scheduled_time": (datetime.now(timezone.utc) + timedelta(minutes=scheduled_minutes)).isoformat(),
            "payload": payload,
            "priority": payload.get("priority", "MEDIUM"),
        }

    def purchase_order_created_handler(context):
        return {
            "status": "SUCCESS",
            "created_events": [
                create_event(
                    "PurchaseOrderApproved",
                    "purchase_orders",
                    "PO-1001",
                    context.get("correlation_id") or "corr-purchase-order",
                    {"po_id": "PO-1001", "priority": "HIGH"},
                    120,
                )
            ],
        }

    def purchase_order_approved_handler(context):
        return {
            "status": "SUCCESS",
            "created_events": [
                create_event(
                    "SupplierShipmentCreated",
                    "shipments",
                    "SHIP-2001",
                    context.get("correlation_id") or "corr-purchase-order",
                    {"shipment_id": "SHIP-2001", "priority": "MEDIUM"},
                    240,
                )
            ],
        }

    def supplier_shipment_created_handler(context):
        return {
            "status": "SUCCESS",
            "created_events": [
                create_event(
                    "ShipmentInTransit",
                    "shipments",
                    "SHIP-2001",
                    context.get("correlation_id") or "corr-purchase-order",
                    {"shipment_id": "SHIP-2001", "priority": "MEDIUM"},
                    180,
                )
            ],
        }

    def shipment_in_transit_handler(context):
        return {
            "status": "SUCCESS",
            "created_events": [
                create_event(
                    "ShipmentDelivered",
                    "shipments",
                    "SHIP-2001",
                    context.get("correlation_id") or "corr-purchase-order",
                    {"shipment_id": "SHIP-2001", "priority": "LOW"},
                    360,
                )
            ],
        }

    return {
        "PurchaseOrderCreated": purchase_order_created_handler,
        "PurchaseOrderApproved": purchase_order_approved_handler,
        "SupplierShipmentCreated": supplier_shipment_created_handler,
        "ShipmentInTransit": shipment_in_transit_handler,
        "ShipmentDelivered": lambda context: {"status": "SUCCESS", "created_events": []},
    }


def enqueue_initial_events(queue):
    now = datetime.now(timezone.utc)
    queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-1001",
        correlation_id="corr-purchase-order",
        payload={
            "po_id": "PO-1001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time=now.isoformat(),
        priority="HIGH",
    )


def main():
    queue = EventQueue(memory_mode=True)
    enqueue_initial_events(queue)
    registry = make_registry()

    checkpoint_path = ROOT / "output" / "last_simulation_checkpoint.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    batches = []
    current_time = datetime.now(timezone.utc)

    for _ in range(10):
        batch = run_simulation_cycle(
            simulation_time=current_time,
            queue=queue,
            checkpoint_path=str(checkpoint_path),
            registry=registry,
        )
        batches.append(batch)

        ready = queue.list_ready_events(current_time.isoformat())
        if not ready:
            break

        current_time += timedelta(minutes=30)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "simulation_start": batches[0]["simulation_time"] if batches else current_time.isoformat(),
        "batches": batches,
        "total_batches": len(batches),
        "queue_size": len(queue._memory_events),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"status": "ok", "output_file": str(OUTPUT_PATH), "batches": len(batches)}, indent=2))


if __name__ == "__main__":
    main()
