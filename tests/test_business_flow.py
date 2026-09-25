import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from event_handlers.purchase_order_approved import handle as handle_purchase_order_approved
from event_handlers.purchase_order_created import handle as handle_purchase_order_created
from event_handlers.supplier_shipment_created import handle as handle_supplier_shipment_created


def test_inbound_event_lifecycle_completes_with_scheduled_next_events(monkeypatch):
    def fake_purchase_order():
        return {
            "po_id": "PO-1001",
            "correlation_id": "corr-1001",
            "created_events": [{
                "event_type": "PurchaseOrderApproved",
                "aggregate_type": "purchase_orders",
                "aggregate_id": "PO-1001",
                "correlation_id": "corr-1001",
                "scheduled_time": "2026-09-20T11:00:00+00:00",
                "payload": {"po_id": "PO-1001", "priority": "HIGH"},
            }],
        }

    def fake_approval():
        return {
            "po_id": "PO-1001",
            "correlation_id": "corr-1001",
            "created_events": [{
                "event_type": "SupplierShipmentCreated",
                "aggregate_type": "shipments",
                "aggregate_id": "SHIP-2001",
                "correlation_id": "corr-1001",
                "scheduled_time": "2026-09-20T12:00:00+00:00",
                "payload": {"shipment_id": "SHIP-2001", "priority": "MEDIUM"},
            }],
        }

    def fake_shipment():
        return {
            "shipment_id": "SHIP-2001",
            "correlation_id": "corr-1001",
            "created_events": [],
        }

    monkeypatch.setattr("generators.purchase_order.purchase_order_created.generate_purchase_order", fake_purchase_order)
    monkeypatch.setattr("generators.purchase_order.purchase_order_approved.approve_purchase_order", fake_approval)
    monkeypatch.setattr("generators.supplier.supplier_shipment_created.create_supplier_shipment", fake_shipment)

    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)
    registry = {
        "PurchaseOrderCreated": handle_purchase_order_created,
        "PurchaseOrderApproved": handle_purchase_order_approved,
        "SupplierShipmentCreated": handle_supplier_shipment_created,
    }

    first = executor.execute(
        {
            "event_id": "evt-1",
            "event_type": "PurchaseOrderCreated",
            "aggregate_type": "purchase_orders",
            "aggregate_id": "PO-1001",
            "correlation_id": "corr-1001",
            "payload": {
                "po_id": "PO-1001",
                "entity_type": "PurchaseOrder",
                "current_state": "CREATED",
                "next_state": "APPROVED",
                "priority": "HIGH",
            },
        },
        registry=registry,
    )

    second = executor.execute(
        {
            "event_id": "evt-2",
            "event_type": "PurchaseOrderApproved",
            "aggregate_type": "purchase_orders",
            "aggregate_id": "PO-1001",
            "correlation_id": "corr-1001",
            "payload": {
                "po_id": "PO-1001",
                "entity_type": "PurchaseOrder",
                "current_state": "CREATED",
                "next_state": "APPROVED",
                "priority": "MEDIUM",
            },
        },
        registry=registry,
    )

    third = executor.execute(
        {
            "event_id": "evt-3",
            "event_type": "SupplierShipmentCreated",
            "aggregate_type": "shipments",
            "aggregate_id": "SHIP-2001",
            "correlation_id": "corr-1001",
            "payload": {
                "shipment_id": "SHIP-2001",
                "entity_type": "Shipment",
                "current_state": "CREATED",
                "next_state": "ASN_RECEIVED",
                "priority": "MEDIUM",
            },
        },
        registry=registry,
    )

    assert first["status"] == "COMPLETED"
    assert second["status"] == "COMPLETED"
    assert third["status"] == "COMPLETED"
    assert first["result"]["created_events"]
    assert second["result"]["created_events"]
