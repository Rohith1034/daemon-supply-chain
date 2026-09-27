import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR_ROOT = ROOT / "simulator"
for path in (ROOT, SIMULATOR_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from core.db import Database
from loading_scripts.domain_flow_runner import run_domain_flow
from loading_scripts.reset_transaction_tables import (
    MASTER_TABLES,
    TRANSACTION_TABLES,
    reset_transaction_tables,
)


OUTPUT_PATH = ROOT / "output" / "independent_flow_validation.json"
EVENT_SEQUENCES = {
    "inbound": [
        "PurchaseOrderCreated", "PurchaseOrderApproved", "SupplierShipmentCreated",
        "ASNReceived", "SupplierShipmentDelivered", "ReceivingTaskCreated",
        "ReceivingTaskStarted", "GoodsReceived", "StockIncreased", "InventoryPutaway",
    ],
    "outbound": [
        "OrderCreated", "OrderItemCreated", "InventoryAllocationCreated", "InventoryReserved",
        "PickingTaskCreated", "PickingTaskStarted", "PickingCompleted", "PackingTaskCreated",
        "PackingTaskStarted", "PackingCompleted", "ShipmentReady", "CarrierAssigned",
        "ShipmentPickedUp", "ShipmentDelivered",
    ],
    "transportation": [
        "CarrierAssigned", "ShipmentPickedUp", "ShipmentInTransit", "ShipmentDelivered",
    ],
}


def _counts(tables):
    with Database() as db:
        return {
            table: db.fetch_one(f"SELECT COUNT(*) AS row_count FROM {table}")["row_count"]
            for table in sorted(tables)
        }


def _event_rows():
    with Database() as db:
        return db.fetch_all(
            """SELECT event_id, event_type, aggregate_type, aggregate_id,
                      correlation_id, payload, created_at
               FROM event_outbox
               ORDER BY created_at, id"""
        )


def _parse_time(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _event_id_from_payload(payload):
    candidates = [payload.get("inventory_id")]
    for key in ("inventory", "stock", "putaway"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidates.append(nested.get("inventory_id"))
    for value in candidates:
        if value not in (None, "", "None", "null"):
            return str(value)
    return None


def _validate_event_contract(domain, events, errors, warnings):
    if not events:
        errors.append("event_outbox contains no events")
        return

    expected = EVENT_SEQUENCES[domain]
    event_types = [event["event_type"] for event in events]
    correlations = {str(event.get("correlation_id")) for event in events if event.get("correlation_id")}
    if len(correlations) != 1:
        errors.append(f"lifecycle correlation IDs are inconsistent: {sorted(correlations)}")
    unexpected = sorted(set(event_types) - set(expected))
    if domain == "outbound" and "ShipmentInTransit" in unexpected:
        unexpected.remove("ShipmentInTransit")
        warnings.append(
            "ShipmentInTransit is emitted between ShipmentPickedUp and ShipmentDelivered "
            "because the existing delivery generator requires IN_TRANSIT state"
        )
    missing = sorted(set(expected) - set(event_types))
    if unexpected:
        errors.append(f"cross-domain or unexpected events: {unexpected}")
    if missing:
        errors.append(f"required lifecycle events missing: {missing}")

    order_positions = {event_type: index for index, event_type in enumerate(expected)}
    positions = [order_positions[event_type] for event_type in event_types if event_type in order_positions]
    if positions != sorted(positions):
        errors.append(f"causal event ordering failed: {event_types}")
    if domain == "outbound" and "ShipmentInTransit" in event_types:
        pickup_index = event_types.index("ShipmentPickedUp") if "ShipmentPickedUp" in event_types else -1
        transit_index = event_types.index("ShipmentInTransit")
        delivered_index = event_types.index("ShipmentDelivered") if "ShipmentDelivered" in event_types else -1
        if not pickup_index < transit_index < delivered_index:
            errors.append("ShipmentInTransit must occur between ShipmentPickedUp and ShipmentDelivered")

    seen = set()
    for event in events:
        for field in ("event_id", "correlation_id", "aggregate_id", "aggregate_type"):
            if event.get(field) in (None, "", "None", "null"):
                errors.append(f"{event.get('event_type')} missing {field}")
        duplicate_key = (
            event.get("event_type"), event.get("aggregate_type"),
            event.get("aggregate_id"), str(event.get("correlation_id")),
        )
        if duplicate_key in seen:
            errors.append(f"duplicate business event: {duplicate_key}")
        seen.add(duplicate_key)

        payload = event.get("payload") or {}
        business_time = (
            payload.get("occurred_at")
            or payload.get("occurredAt")
            or payload.get("simulation_time")
            or payload.get("simulationTime")
        )
        if not business_time:
            errors.append(f"{event.get('event_type')} missing occurred_at/simulation_time")
        else:
            try:
                business_timestamp = _parse_time(business_time)
                database_timestamp = _parse_time(event["created_at"])
                if business_timestamp == database_timestamp:
                    errors.append(f"{event.get('event_type')} business time equals database insertion time")
            except (TypeError, ValueError):
                errors.append(f"{event.get('event_type')} has invalid business timestamp {business_time!r}")
        if event.get("created_at") is None:
            errors.append(f"{event.get('event_type')} missing database created_at")


def _fetch_one(query, params):
    with Database() as db:
        return db.fetch_one(query, params)


def _fetch_all(query, params):
    with Database() as db:
        return db.fetch_all(query, params)


def _validate_inbound(events, errors, sql_results):
    stock_rows = [event for event in events if event["event_type"] == "StockIncreased"]
    putaway_rows = [event for event in events if event["event_type"] == "InventoryPutaway"]
    stock_ids = [_event_id_from_payload(event.get("payload") or {}) for event in stock_rows]
    putaway_ids = [_event_id_from_payload(event.get("payload") or {}) for event in putaway_rows]
    if len(stock_rows) != len(putaway_rows):
        errors.append(f"StockIncreased/InventoryPutaway counts differ: {len(stock_rows)} != {len(putaway_rows)}")
    if None in stock_ids or None in putaway_ids or sorted(stock_ids) != sorted(putaway_ids):
        errors.append(f"inventory IDs do not match between stock and putaway events: {stock_ids} != {putaway_ids}")

    inventory_rows = _fetch_all(
          """SELECT i.inventory_id, i.inventory_status, i.location_id,
                  wl.location_id AS valid_location_id
           FROM inventory i
           LEFT JOIN warehouse_locations wl ON wl.location_id=i.location_id
              WHERE i.inventory_id::text = ANY(%s)""",
        (stock_ids,),
    ) if stock_ids and None not in stock_ids else []
    sql_results["inbound_inventory"] = [dict(row) for row in inventory_rows]
    if len(inventory_rows) != len(set(stock_ids)):
        errors.append("one or more StockIncreased inventory rows are missing")
    for row in inventory_rows:
        if row["inventory_status"] != "AVAILABLE":
            errors.append(f"inventory {row['inventory_id']} status is {row['inventory_status']}, expected AVAILABLE")
        if not row["location_id"] or not row["valid_location_id"]:
            errors.append(f"inventory {row['inventory_id']} has no valid putaway location")

    if events:
        correlation_id = str(events[0]["correlation_id"])
        lifecycle_rows = _fetch_all(
            """SELECT po.po_id, shipment.shipment_id, shipment.po_id AS shipment_po_id,
                      task.task_id, task.shipment_id AS task_shipment_id, task.status AS task_status
               FROM purchase_orders po
               LEFT JOIN shipments shipment ON shipment.po_id=po.po_id
               LEFT JOIN warehouse_tasks task
                 ON task.shipment_id=shipment.shipment_id AND task.task_type='RECEIVING'
               WHERE po.correlation_id=%s
               ORDER BY po.po_id, shipment.shipment_id, task.task_id""",
            (correlation_id,),
        )
        sql_results["inbound_parent_chain"] = [dict(row) for row in lifecycle_rows]
        if not lifecycle_rows or any(not row["shipment_id"] for row in lifecycle_rows):
            errors.append("inbound purchase order has no supplier shipment child")
        if any(row["shipment_po_id"] != row["po_id"] for row in lifecycle_rows if row["shipment_id"]):
            errors.append("inbound shipment references an unknown purchase order")
        if any(not row["task_id"] for row in lifecycle_rows):
            errors.append("inbound shipment has no receiving task child")
        if any(row["task_shipment_id"] != row["shipment_id"] for row in lifecycle_rows if row["task_id"]):
            errors.append("inbound receiving task references an unknown shipment")
        if any(row["task_status"] != "COMPLETED" for row in lifecycle_rows if row["task_id"]):
            errors.append("inbound receiving task is not completed")


def _validate_outbound(result, errors, sql_results):
    order_id = result.get("order_id")
    order = _fetch_one(
        "SELECT order_id, correlation_id FROM orders WHERE order_id=%s",
        (order_id,),
    ) if order_id else None
    if not order:
        errors.append("outbound order root is missing")
        return

    allocations = _fetch_all(
        "SELECT allocation_id, order_id, product_id, inventory_id, allocation_status FROM inventory_allocations WHERE order_id=%s",
        (order_id,),
    )
    reservations = _fetch_all(
        "SELECT order_id, product_id, reservation_status FROM inventory_reservations WHERE order_id=%s",
        (order_id,),
    )
    picking = _fetch_all(
        "SELECT task_id, order_id, allocation_id, status FROM warehouse_tasks WHERE order_id=%s AND task_type='PICKING'",
        (order_id,),
    )
    packing = _fetch_all(
        "SELECT task_id, order_id, picking_task_id, status FROM warehouse_tasks WHERE order_id=%s AND task_type='PACKING'",
        (order_id,),
    )
    packages = _fetch_all(
        "SELECT package_id, order_id, package_status FROM packages WHERE order_id=%s",
        (order_id,),
    )
    shipments = _fetch_all(
        "SELECT shipment_id, fulfillment_id, order_id, package_id, shipment_status FROM outbound_shipments WHERE order_id=%s",
        (order_id,),
    )
    sql_results["outbound_relationships"] = {
        "order": dict(order),
        "allocations": [dict(row) for row in allocations],
        "reservations": [dict(row) for row in reservations],
        "picking_tasks": [dict(row) for row in picking],
        "packing_tasks": [dict(row) for row in packing],
        "packages": [dict(row) for row in packages],
        "shipments": [dict(row) for row in shipments],
    }
    if not allocations or any(row["order_id"] != order_id for row in allocations):
        errors.append("order-to-allocation relationship is invalid")
    if not reservations:
        errors.append("outbound order has no inventory reservation")
    allocated_products = {row["product_id"] for row in allocations}
    reserved_products = {row["product_id"] for row in reservations if row["reservation_status"] == "RESERVED"}
    if allocated_products != reserved_products:
        errors.append(f"allocation/reservation product mismatch: {allocated_products} != {reserved_products}")
    if not picking or any(row["status"] != "COMPLETED" for row in picking):
        errors.append("outbound picking tasks are missing or incomplete")
    allocation_ids = {row["allocation_id"] for row in allocations}
    if any(row["allocation_id"] not in allocation_ids for row in picking):
        errors.append("picking task references an unknown allocation")
    picking_ids = {row["task_id"] for row in picking}
    if not packing or any(row["status"] != "COMPLETED" for row in packing):
        errors.append("outbound packing tasks are missing or incomplete")
    if any(row["picking_task_id"] not in picking_ids for row in packing):
        errors.append("packing task references an unknown picking task")
    package_ids = {row["package_id"] for row in packages}
    if not packages or any(row["package_status"] != "PACKED" for row in packages):
        errors.append("packed package is missing or incomplete")
    if not shipments or any(row["package_id"] not in package_ids for row in shipments):
        errors.append("shipment references a missing package")
    if any(not row["fulfillment_id"] for row in shipments):
        errors.append("shipment has no outbound fulfillment parent")
    if shipments and any(row["shipment_status"] != "DELIVERED" for row in shipments):
        errors.append("outbound shipment is not delivered")


def _validate_transportation(result, errors, sql_results):
    shipment_id = result.get("shipment_id")
    shipment = _fetch_one(
        """SELECT os.shipment_id, os.shipment_status, os.order_id, os.package_id,
                  ost.status AS transportation_status, ost.vehicle_id, ost.trailer_id,
                  ost.driver_id, tracking.status AS tracking_status
           FROM outbound_shipments os
           LEFT JOIN outbound_shipment_transportation ost ON ost.shipment_id=os.shipment_id
           LEFT JOIN LATERAL (
               SELECT status FROM outbound_shipment_tracking
               WHERE shipment_id=os.shipment_id ORDER BY created_at DESC LIMIT 1
           ) tracking ON TRUE
           WHERE os.shipment_id=%s""",
        (shipment_id,),
    ) if shipment_id else None
    sql_results["transportation_relationship"] = dict(shipment) if shipment else None
    if not shipment:
        errors.append("transportation shipment root is missing")
        return
    if shipment["shipment_status"] != "DELIVERED":
        errors.append(f"transportation shipment status is {shipment['shipment_status']}, expected DELIVERED")
    if not all(shipment.get(field) for field in ("vehicle_id", "trailer_id", "driver_id")):
        errors.append("transportation assignment is missing vehicle, trailer, or driver")
    if shipment.get("tracking_status") != "DELIVERED":
        errors.append(f"transportation tracking status is {shipment.get('tracking_status')}, expected DELIVERED")


def validate_domain(domain):
    reset_result = reset_transaction_tables()
    transaction_counts_after_reset = _counts(TRANSACTION_TABLES)
    master_counts_after_reset = _counts(MASTER_TABLES)
    master_counts_before_run = _counts(MASTER_TABLES)
    correlation_id = str(uuid.uuid4())
    import os

    os.environ["SIMULATION_CORRELATION_ID"] = correlation_id
    result = run_domain_flow(domain)
    master_counts_after_run = _counts(MASTER_TABLES)
    events = _event_rows()
    errors = []
    warnings = []
    sql_results = {
        "master_counts_after_reset": master_counts_after_reset,
        "master_counts_before_run": master_counts_before_run,
        "master_counts_after_run": master_counts_after_run,
        "transaction_counts_after_reset": transaction_counts_after_reset,
        "transaction_counts_after_run": _counts(TRANSACTION_TABLES),
        "event_rows": [dict(event) for event in events],
        "duplicate_business_event_groups": _fetch_all(
            """SELECT event_type, aggregate_type, aggregate_id, correlation_id, COUNT(*) AS duplicate_count
               FROM event_outbox
               GROUP BY event_type, aggregate_type, aggregate_id, correlation_id
               HAVING COUNT(*) > 1
               ORDER BY event_type, aggregate_type, aggregate_id, correlation_id""",
            (),
        ),
        "events_missing_contract_fields": _fetch_all(
            """SELECT event_id, event_type
               FROM event_outbox
               WHERE event_id IS NULL OR correlation_id IS NULL OR aggregate_id IS NULL OR aggregate_type IS NULL
               ORDER BY event_type""",
            (),
        ),
    }
    sql_results["duplicate_business_event_groups"] = [dict(row) for row in sql_results["duplicate_business_event_groups"]]
    sql_results["events_missing_contract_fields"] = [dict(row) for row in sql_results["events_missing_contract_fields"]]

    if sql_results["master_counts_after_reset"] != reset_result["master_counts"]:
        errors.append("required master counts changed after reset")
    if master_counts_after_run != master_counts_before_run:
        errors.append(
            "required master counts changed during lifecycle execution: "
            f"before={master_counts_before_run}, after={master_counts_after_run}"
        )
    if any(sql_results["transaction_counts_after_reset"].values()):
        errors.append("transactional tables were not empty after reset")
    if sql_results["duplicate_business_event_groups"]:
        errors.append(f"SQL duplicate business-event groups found: {sql_results['duplicate_business_event_groups']}")
    if sql_results["events_missing_contract_fields"]:
        errors.append(f"SQL events missing contract fields: {sql_results['events_missing_contract_fields']}")

    _validate_event_contract(domain, events, errors, warnings)
    if domain == "inbound":
        _validate_inbound(events, errors, sql_results)
    elif domain == "outbound":
        _validate_outbound(result, errors, sql_results)
    else:
        _validate_transportation(result, errors, sql_results)

    return {
        "status": "PASS" if not errors else "FAIL",
        "domain": domain,
        "correlation_id": correlation_id,
        "result": result,
        "errors": errors,
        "warnings": warnings,
        "sql_validation": sql_results,
    }


def main(cycles=3):
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "cycles": []}
    for cycle in range(1, cycles + 1):
        cycle_report = {"cycle": cycle, "domains": []}
        for domain in ("inbound", "outbound", "transportation"):
            print(f"RUN {cycle}: {domain} only")
            try:
                result = validate_domain(domain)
            except Exception as exc:
                result = {"status": "FAIL", "domain": domain, "errors": [str(exc)]}
            cycle_report["domains"].append(result)
            print(f"{domain}: {result['status']}")
            for error in result.get("errors", []):
                print(f"  ERROR: {error}")
            for warning in result.get("warnings", []):
                print(f"  WARNING: {warning}")
            OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT_PATH.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        report["cycles"].append(cycle_report)
        OUTPUT_PATH.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    passed = sum(
        domain["status"] == "PASS"
        for cycle in report["cycles"]
        for domain in cycle["domains"]
    )
    total = cycles * 3
    print(f"Validation summary: {passed}/{total} domain runs passed")
    print(f"Full SQL-backed report: {OUTPUT_PATH}")
    return 0 if passed == total else 1


if __name__ == "__main__":
    requested_cycles = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    if requested_cycles < 1:
        raise SystemExit("cycles must be a positive integer")
    raise SystemExit(main(requested_cycles))