from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, ContextManager

from fastapi import HTTPException

ROOT_DIR = Path(__file__).resolve().parents[2]
SIMULATOR_DIR = ROOT_DIR / "simulator"
RUNNER_PATH = SIMULATOR_DIR / "loading_scripts" / "domain_flow_runner.py"
RUN_STORE = ROOT_DIR / "output" / "simulation_runs"

if str(SIMULATOR_DIR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_DIR))

from core.db import Database
from api.schemas.simulator_schema import (
    IndependentSimulationRequest,
    IndependentSimulationResponse,
)
from api.services.run_lock import simulation_execution_lock


@dataclass(frozen=True)
class RunContext:
    simulation_id: str
    correlation_id: uuid.UUID
    flow: str
    simulation_timestamp: datetime


def _utc(value: datetime | None) -> datetime:
    if value is None:
        value = datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _root_business_timestamp(flow: str, payload: dict) -> str | None:
    if flow == "inbound":
        purchase_order = payload.get("purchaseOrder") or {}
        return (
            purchase_order.get("orderDate")
            or payload.get("occurred_at")
            or payload.get("occurredAt")
        )
    return payload.get("occurred_at") or payload.get("occurredAt")


def _token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _run_directory(root: Path, simulation_id: str) -> Path:
    return root / _token(simulation_id)


def _correlation_index(root: Path, flow: str, correlation_id: uuid.UUID) -> Path:
    key = f"{flow}:{correlation_id}"
    return root / "_correlations" / f"{_token(key)}.json"


def _root_exists(context: RunContext) -> bool:
    if context.flow == "inbound":
        with Database() as db:
            return bool(db.fetch_one(
                """SELECT EXISTS (
                       SELECT 1 FROM purchase_orders WHERE correlation_id=%s
                   ) OR EXISTS (
                       SELECT 1 FROM event_outbox
                       WHERE correlation_id=%s AND event_type='PurchaseOrderCreated'
                   ) AS found""",
                (context.correlation_id, str(context.correlation_id)),
            )["found"])
    if context.flow == "outbound":
        with Database() as db:
            return bool(db.fetch_one(
                """SELECT EXISTS (
                       SELECT 1 FROM orders WHERE correlation_id=%s
                   ) OR EXISTS (
                       SELECT 1 FROM event_outbox
                       WHERE correlation_id=%s AND event_type='OrderCreated'
                   ) AS found""",
                (context.correlation_id, str(context.correlation_id)),
            )["found"])
    with Database() as db:
        return bool(db.fetch_one(
            """SELECT EXISTS (
                   SELECT 1 FROM outbound_shipments WHERE correlation_id=%s
               ) OR EXISTS (
                   SELECT 1 FROM event_outbox
                   WHERE correlation_id=%s AND event_type='CarrierAssigned'
               ) AS found""",
            (context.correlation_id, str(context.correlation_id)),
        )["found"])


def _validate_completed_flow(context: RunContext, result: dict) -> list[str]:
    errors = []
    with Database() as db:
        events = db.fetch_all(
            """SELECT event_id,event_type,aggregate_type,aggregate_id,correlation_id,payload,created_at
               FROM event_outbox WHERE correlation_id=%s ORDER BY created_at,id""",
            (str(context.correlation_id),),
        )
        duplicates = db.fetch_all(
            """SELECT event_type,aggregate_type,aggregate_id,COUNT(*) AS duplicate_count
               FROM event_outbox WHERE correlation_id=%s
               GROUP BY event_type,aggregate_type,aggregate_id,correlation_id
               HAVING COUNT(*)>1""",
            (str(context.correlation_id),),
        )

        if not events:
            return ["no outbox events were persisted for this correlation"]
        if duplicates:
            errors.append("duplicate business event identities were persisted")
        if any(
            not event.get(field)
            for event in events
            for field in ("event_id", "event_type", "aggregate_type", "aggregate_id", "correlation_id")
        ):
            errors.append("one or more events are missing a required identity field")
        if any(str(event["correlation_id"]) != str(context.correlation_id) for event in events):
            errors.append("persisted event correlation IDs do not match the run correlation ID")

        event_types = [event["event_type"] for event in events]
        root_type = {
            "inbound": "PurchaseOrderCreated",
            "outbound": "OrderCreated",
            "transportation": "CarrierAssigned",
        }[context.flow]
        root = next((event for event in events if event["event_type"] == root_type), None)
        if root is None:
            errors.append(f"root event {root_type} is missing")
        else:
            payload = dict(root.get("payload") or {})
            business_timestamp = _root_business_timestamp(context.flow, payload)
            try:
                if business_timestamp is None or _utc(datetime.fromisoformat(str(business_timestamp).replace("Z", "+00:00"))) != context.simulation_timestamp:
                    errors.append(f"{root_type} payload business timestamp differs from simulation_timestamp")
            except (TypeError, ValueError):
                errors.append(f"{root_type} payload has an invalid business timestamp")
            if root.get("created_at") is not None and _utc(root["created_at"]) == context.simulation_timestamp:
                errors.append("database created_at was replaced with simulation time")

        if context.flow == "inbound":
            fixed = [
                "PurchaseOrderCreated", "PurchaseOrderApproved", "SupplierShipmentCreated",
                "ASNReceived", "SupplierShipmentDelivered", "ReceivingTaskCreated",
                "ReceivingTaskStarted", "GoodsReceived",
            ]
            purchase_order = db.fetch_one(
                "SELECT po_id,order_date FROM purchase_orders WHERE correlation_id=%s ORDER BY created_at LIMIT 1",
                (context.correlation_id,),
            )
            item_count = db.fetch_one(
                "SELECT COUNT(*) AS n FROM purchase_order_items WHERE po_id=%s",
                (purchase_order["po_id"],),
            )["n"] if purchase_order else 0
            stock_count = event_types.count("StockIncreased")
            putaway_count = event_types.count("InventoryPutaway")
            expected_types = fixed + ["StockIncreased"] * item_count + ["InventoryPutaway"] * item_count
            if event_types != expected_types or item_count == 0 or stock_count != putaway_count or stock_count != item_count:
                errors.append("inbound event order or item-level StockIncreased/InventoryPutaway counts are incomplete")
            if purchase_order is None or _utc(purchase_order["order_date"]) != context.simulation_timestamp:
                errors.append("inbound purchase order business time differs from simulation_timestamp")
            inventory = db.fetch_all(
                """SELECT i.inventory_status,i.location_id,wl.location_id AS valid_location_id
                   FROM inventory i LEFT JOIN warehouse_locations wl ON wl.location_id=i.location_id
                   WHERE i.correlation_id=%s""",
                (context.correlation_id,),
            )
            if len(inventory) != item_count or any(
                row["inventory_status"] != "AVAILABLE" or not row["location_id"] or not row["valid_location_id"]
                for row in inventory
            ):
                errors.append("inbound inventory is not fully AVAILABLE and put away to valid locations")
            if purchase_order:
                parents = db.fetch_one(
                    """SELECT COUNT(*) AS n
                       FROM shipments s
                       JOIN warehouse_tasks t ON t.shipment_id=s.shipment_id AND t.task_type='RECEIVING'
                       WHERE s.po_id=%s AND t.status='COMPLETED'""",
                    (purchase_order["po_id"],),
                )
                if not parents or parents["n"] == 0:
                    errors.append("inbound shipment/receiving-task parent chain is incomplete")

        elif context.flow == "outbound":
            expected = [
                "OrderCreated", "OrderItemCreated", "InventoryAllocationCreated", "InventoryReserved",
                "PickingTaskCreated", "PickingTaskStarted", "PickingCompleted", "PackingTaskCreated",
                "PackingTaskStarted", "PackingCompleted", "ShipmentReady", "CarrierAssigned",
                "ShipmentPickedUp", "ShipmentInTransit", "ShipmentDelivered",
            ]
            if event_types != expected:
                errors.append("outbound event chain is incomplete or out of order")
            order = db.fetch_one(
                "SELECT order_id,order_date FROM orders WHERE correlation_id=%s ORDER BY created_at LIMIT 1",
                (context.correlation_id,),
            )
            if order is None:
                errors.append("outbound order root was not persisted")
            else:
                if _utc(order["order_date"]) != context.simulation_timestamp:
                    errors.append("outbound order business time differs from simulation_timestamp")
                item_coverage = db.fetch_all(
                    """SELECT oi.product_id,oi.quantity,
                              COALESCE((SELECT SUM(a.allocated_quantity) FROM inventory_allocations a
                                        WHERE a.order_id=oi.order_id AND a.product_id=oi.product_id),0) AS allocated,
                              COALESCE((SELECT SUM(r.quantity) FROM inventory_reservations r
                                        WHERE r.order_id=oi.order_id AND r.product_id=oi.product_id
                                          AND r.reservation_status='RESERVED'),0) AS reserved
                       FROM order_items oi WHERE oi.order_id=%s""",
                    (order["order_id"],),
                )
                if not item_coverage or any(
                    row["quantity"] != row["allocated"] or row["quantity"] != row["reserved"]
                    for row in item_coverage
                ):
                    errors.append("one or more outbound order items are not fully allocated and reserved")
                relationships = db.fetch_one(
                    """SELECT
                           (SELECT COUNT(*) FROM warehouse_tasks t WHERE t.order_id=%s AND t.task_type='PICKING' AND t.status='COMPLETED') AS picks,
                           (SELECT COUNT(*) FROM warehouse_tasks p WHERE p.order_id=%s AND p.task_type='PACKING' AND p.status='COMPLETED'
                             AND EXISTS (SELECT 1 FROM warehouse_tasks t WHERE t.task_id=p.picking_task_id AND t.task_type='PICKING' AND t.order_id=p.order_id)) AS packs,
                           (SELECT COUNT(*) FROM outbound_shipments s WHERE s.order_id=%s AND s.shipment_status='DELIVERED'
                             AND EXISTS (SELECT 1 FROM packages p WHERE p.package_id=s.package_id AND p.order_id=s.order_id AND p.package_status='PACKED')
                             AND EXISTS (SELECT 1 FROM outbound_fulfillment f WHERE f.fulfillment_id=s.fulfillment_id AND f.order_id=s.order_id)) AS shipments""",
                    (order["order_id"], order["order_id"], order["order_id"]),
                )
                if not relationships or any(relationships[key] == 0 for key in ("picks", "packs", "shipments")):
                    errors.append("outbound picking, packing, or shipment relationships are incomplete")

        else:
            expected = ["CarrierAssigned", "ShipmentPickedUp", "ShipmentInTransit", "ShipmentDelivered"]
            if event_types != expected:
                errors.append("transportation event chain is incomplete or out of order")
            shipment = db.fetch_one(
                """SELECT s.shipment_id,s.shipment_date,s.shipment_status,
                          t.status AS transportation_status,t.vehicle_id,t.trailer_id,t.driver_id,
                          tr.status AS tracking_status
                   FROM outbound_shipments s
                   LEFT JOIN outbound_shipment_transportation t ON t.shipment_id=s.shipment_id
                   LEFT JOIN LATERAL (
                       SELECT status FROM outbound_shipment_tracking
                       WHERE shipment_id=s.shipment_id ORDER BY created_at DESC LIMIT 1
                   ) tr ON TRUE
                   WHERE s.correlation_id=%s ORDER BY s.created_at LIMIT 1""",
                (context.correlation_id,),
            )
            if shipment is None:
                errors.append("transportation shipment root was not persisted")
            else:
                if _utc(shipment["shipment_date"]) != context.simulation_timestamp:
                    errors.append("transportation shipment business time differs from simulation_timestamp")
                if shipment["shipment_status"] != "DELIVERED" or shipment["transportation_status"] != "DELIVERED" or shipment["tracking_status"] != "DELIVERED":
                    errors.append("transportation assignment/pickup/transit/delivery is incomplete")
                if any(shipment[field] is None for field in ("vehicle_id", "trailer_id", "driver_id")):
                    errors.append("transportation assignment is missing a vehicle, trailer, or driver")

    return errors


def _extract_runner_result(stdout: str, expected_flow: str) -> dict:
    decoder = json.JSONDecoder()
    offset = 0
    while True:
        start = stdout.find("{", offset)
        if start < 0:
            break
        try:
            value, _end = decoder.raw_decode(stdout, start)
        except json.JSONDecodeError:
            offset = start + 1
            continue
        if isinstance(value, dict) and value.get("domain") == expected_flow:
            return value
        offset = start + 1
    raise RuntimeError("Flow runner returned invalid JSON")


def _execute_runner(context: RunContext) -> dict:
    environment = os.environ.copy()
    environment["SIMULATION_ID"] = context.simulation_id
    environment["SIMULATION_CORRELATION_ID"] = str(context.correlation_id)
    environment["SIMULATION_NOW"] = context.simulation_timestamp.isoformat()
    pythonpath = [str(SIMULATOR_DIR), str(ROOT_DIR)]
    if environment.get("PYTHONPATH"):
        pythonpath.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(pythonpath)

    result = subprocess.run(
        [sys.executable, str(RUNNER_PATH), context.flow],
        cwd=ROOT_DIR,
        env=environment,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip() or "Flow runner failed"
        raise RuntimeError(details[-4000:])

    payload = _extract_runner_result(result.stdout, context.flow)
    if payload.get("domain") != context.flow:
        raise RuntimeError("Flow runner returned a different domain")
    if payload.get("correlation_id") != str(context.correlation_id):
        raise RuntimeError("Flow runner correlation ID differs from the request")
    if any(event.get("status") != "SUCCESS" for event in payload.get("events", [])):
        raise RuntimeError("Flow runner returned an incomplete event result")
    return payload


class IndependentSimulationService:
    def __init__(
        self,
        run_store: Path = RUN_STORE,
        runner: Callable[[RunContext], dict] | None = None,
        lock_factory: Callable[[], ContextManager] = simulation_execution_lock,
        root_exists: Callable[[RunContext], bool] = _root_exists,
        validator: Callable[[RunContext, dict], list[str]] = _validate_completed_flow,
    ):
        self.run_store = Path(run_store)
        self.runner = runner or _execute_runner
        self.lock_factory = lock_factory
        self.root_exists = root_exists
        self.validator = validator

    @staticmethod
    def _response(record: dict, *, replayed: bool = False) -> IndependentSimulationResponse:
        response = dict(record["response"])
        response["replayed"] = replayed
        return IndependentSimulationResponse(**response)

    def _persist(self, context: RunContext, record: dict) -> None:
        run_dir = _run_directory(self.run_store, context.simulation_id)
        _write_json(run_dir / "run.json", record)
        _write_json(
            _correlation_index(self.run_store, context.flow, context.correlation_id),
            {"simulation_id": context.simulation_id},
        )

    def _existing_record(self, context: RunContext, request: IndependentSimulationRequest) -> tuple[RunContext, dict] | None:
        index = _read_json(_correlation_index(self.run_store, context.flow, context.correlation_id))
        if index is None:
            run_record = _read_json(_run_directory(self.run_store, context.simulation_id) / "run.json")
            if run_record is None:
                return None
            existing_id = run_record.get("simulation_id")
        else:
            existing_id = index.get("simulation_id")
            run_record = _read_json(_run_directory(self.run_store, str(existing_id)) / "run.json")
        if not run_record:
            return None
        existing_context = RunContext(
            simulation_id=str(existing_id),
            correlation_id=uuid.UUID(run_record["correlation_id"]),
            flow=run_record["flow"],
            simulation_timestamp=_utc(datetime.fromisoformat(run_record["simulation_timestamp"])),
        )
        if request.simulation_id and request.simulation_id != existing_context.simulation_id:
            raise HTTPException(
                status_code=409,
                detail="This correlation ID is already associated with another simulation_id",
            )
        if (existing_context.flow, existing_context.correlation_id) != (context.flow, context.correlation_id):
            raise HTTPException(
                status_code=409,
                detail="simulation_id is already associated with another flow or correlation_id",
            )
        return existing_context, run_record

    def run(self, flow: str, request: IndependentSimulationRequest) -> IndependentSimulationResponse:
        if flow not in {"inbound", "outbound", "transportation"}:
            raise ValueError(f"Unsupported flow: {flow}")
        context = RunContext(
            simulation_id=request.simulation_id or str(uuid.uuid4()),
            correlation_id=request.correlation_id or uuid.uuid4(),
            flow=flow,
            simulation_timestamp=_utc(request.simulation_timestamp),
        )

        with self.lock_factory():
            existing = self._existing_record(context, request)
            if existing:
                context, record = existing
                if record["status"] == "SUCCESS":
                    return self._response(record, replayed=True)
                if record["status"] == "FAILED":
                    if not record.get("retryable"):
                        return self._response(record, replayed=True)
                    if self.root_exists(context):
                        record["retryable"] = False
                        record["response"]["retryable"] = False
                        record["response"]["message"] = (
                            "Partial business state exists; automatic replay is disabled to prevent a duplicate root aggregate"
                        )
                        self._persist(context, record)
                        return self._response(record, replayed=True)
                elif record["status"] == "RUNNING":
                    if self.root_exists(context):
                        record["status"] = "FAILED"
                        record["retryable"] = False
                        record["response"].update({
                            "status": "FAILED",
                            "retryable": False,
                            "message": "Previous run stopped after creating business state; automatic replay is disabled",
                        })
                        self._persist(context, record)
                        return self._response(record, replayed=True)
                context = RunContext(
                    simulation_id=context.simulation_id,
                    correlation_id=context.correlation_id,
                    flow=context.flow,
                    simulation_timestamp=context.simulation_timestamp,
                )

            elif self.root_exists(context):
                raise HTTPException(
                    status_code=409,
                    detail="Business state already exists for this correlation_id; refusing to create another root aggregate",
                )

            run_dir = _run_directory(self.run_store, context.simulation_id)
            response = {
                "status": "FAILED",
                "simulation_id": context.simulation_id,
                "correlation_id": context.correlation_id,
                "flow": context.flow,
                "simulation_timestamp": context.simulation_timestamp,
                "message": "Flow execution failed",
                "retryable": False,
                "replayed": False,
                "report_path": str(run_dir / "result.json"),
                "summary": None,
            }
            record = {
                "status": "RUNNING",
                "retryable": False,
                "simulation_id": context.simulation_id,
                "correlation_id": str(context.correlation_id),
                "flow": context.flow,
                "simulation_timestamp": context.simulation_timestamp.isoformat(),
                "response": response,
            }
            self._persist(context, record)
            runner_result_saved = False
            try:
                result = self.runner(context)
                _write_json(run_dir / "result.json", result)
                runner_result_saved = True
                validation_errors = self.validator(context, result)
                if validation_errors:
                    raise RuntimeError("; ".join(validation_errors))
                summary = {
                    "event_count": len(result.get("events", [])),
                    "events": [event.get("event") for event in result.get("events", [])],
                    "order_id": result.get("order_id"),
                    "shipment_id": result.get("shipment_id"),
                }
                record.update({"status": "SUCCESS", "retryable": False, "result": summary})
                record["response"].update({
                    "status": "SUCCESS",
                    "message": f"{context.flow} flow completed",
                    "summary": summary,
                })
            except Exception as exc:
                try:
                    retryable = not self.root_exists(context)
                except Exception:
                    retryable = False
                record.update({"status": "FAILED", "retryable": retryable, "error": str(exc)})
                record["response"].update({
                    "status": "FAILED",
                    "retryable": retryable,
                    "message": str(exc),
                })
                if not runner_result_saved:
                    _write_json(run_dir / "result.json", {"status": "FAILED", "error": str(exc)})
            self._persist(context, record)
            return self._response(record)


independent_simulation_service = IndependentSimulationService()