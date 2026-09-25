from datetime import datetime, timedelta, timezone
import inspect
import os
import uuid

from core.event_scheduler.event_registry import EVENT_REGISTRY
from core.event_scheduler.event_queue import EventQueue
from core.event_state_machine import validate_transition, infer_next_state, infer_entity_type
from core.event_timing import get_future_event_time
from core.simulation_clock import get_simulation_now
from core.db import Database


EVENT_PROCESSING_LATENCY_SECONDS = int(os.getenv("EVENT_PROCESSING_LATENCY_SECONDS", "1"))


class InvalidStateTransition(Exception):
    pass


class EventExecutor:
    """Execute scheduled events with retry-safe idempotency checks."""

    NEXT_EVENT_SEQUENCE = {
        "PurchaseOrderCreated": {"event_type": "PurchaseOrderApproved", "aggregate_type": "purchase_orders", "entity_type": "PurchaseOrder", "current_state": "CREATED", "next_state": "APPROVED", "priority": "HIGH"},
        "PurchaseOrderApproved": {"event_type": "SupplierShipmentCreated", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "CREATED", "next_state": "READY", "priority": "HIGH"},
        "SupplierShipmentCreated": {"event_type": "ASNReceived", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "CREATED", "next_state": "ASN_RECEIVED", "priority": "MEDIUM"},
        "ASNReceived": {"event_type": "SupplierShipmentDelivered", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "IN_TRANSIT", "next_state": "DELIVERED", "priority": "MEDIUM"},
        "SupplierShipmentDelivered": {"event_type": "ReceivingTaskCreated", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "CREATED", "next_state": "STARTED", "priority": "MEDIUM"},
        "ReceivingTaskCreated": {"event_type": "ReceivingTaskStarted", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "CREATED", "next_state": "STARTED", "priority": "MEDIUM"},
        "ReceivingTaskStarted": {"event_type": "GoodsReceived", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "STARTED", "next_state": "COMPLETED", "priority": "MEDIUM"},
        "GoodsReceived": {"event_type": "StockIncreased", "aggregate_type": "inventory", "entity_type": "Inventory", "current_state": "RECEIVED", "next_state": "AVAILABLE", "priority": "LOW"},
        "StockIncreased": {"event_type": "InventoryPutaway", "aggregate_type": "inventory_locations", "entity_type": "Inventory", "current_state": "READY", "next_state": "PUTAWAY", "priority": "LOW"},
        "InventoryPutaway": {"event_type": "OrderCreated", "aggregate_type": "orders", "entity_type": "Order", "current_state": "CREATED", "next_state": "RESERVED", "priority": "HIGH"},
        "OrderCreated": {"event_type": "OrderItemCreated", "aggregate_type": "order_items", "entity_type": "Order", "current_state": "CREATED", "next_state": "ALLOCATED", "priority": "HIGH"},
        "OrderItemCreated": {"event_type": "InventoryAllocationCreated", "aggregate_type": "inventory_allocations", "entity_type": "Order", "current_state": "PENDING", "next_state": "ALLOCATED", "priority": "HIGH"},
        "InventoryAllocationCreated": {"event_type": "InventoryReserved", "aggregate_type": "inventory_reservations", "entity_type": "Order", "current_state": "ALLOCATED", "next_state": "RESERVED", "priority": "HIGH"},
        "InventoryReserved": {"event_type": "PickingTaskCreated", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "CREATED", "next_state": "PICKING", "priority": "HIGH"},
        "PickingTaskCreated": {"event_type": "PickingTaskStarted", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "CREATED", "next_state": "PICKING", "priority": "HIGH"},
        "PickingTaskStarted": {"event_type": "PickingCompleted", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "PICKING", "next_state": "IN_PROGRESS", "priority": "HIGH"},
        "PickingCompleted": {"event_type": "PackingTaskCreated", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "CREATED", "next_state": "PACKING", "priority": "MEDIUM"},
        "PackingTaskCreated": {"event_type": "PackingCompleted", "aggregate_type": "warehouse_tasks", "entity_type": "WarehouseTask", "current_state": "PACKING", "next_state": "COMPLETED", "priority": "MEDIUM"},
        "PackingCompleted": {"event_type": "ShipmentReady", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "PENDING", "next_state": "READY", "priority": "MEDIUM"},
        "ShipmentReady": {"event_type": "CarrierAssigned", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "READY", "next_state": "ASSIGNED", "priority": "MEDIUM"},
        "CarrierAssigned": {"event_type": "VehicleAssigned", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "ASSIGNED", "next_state": "LOADED", "priority": "MEDIUM"},
        "VehicleAssigned": {"event_type": "ShipmentPickedUp", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "LOADED", "next_state": "IN_TRANSIT", "priority": "MEDIUM"},
        "ShipmentPickedUp": {"event_type": "ShipmentDelivered", "aggregate_type": "shipments", "entity_type": "Shipment", "current_state": "IN_TRANSIT", "next_state": "DELIVERED", "priority": "MEDIUM"},
    }

    DOMAIN_NEXT_EVENT_SEQUENCE = {
        "CustomerOrderCreated": ("InventoryReserved", "inventory", "Inventory", "CREATED", "RESERVED"),
        "PickingTaskCreated": ("PickingStarted", "warehouse_tasks", "WarehouseTask", "CREATED", "STARTED"),
        "PickingStarted": ("PickingCompleted", "warehouse_tasks", "WarehouseTask", "STARTED", "COMPLETED"),
        "PickingCompleted": ("PackingStarted", "warehouse_tasks", "WarehouseTask", "COMPLETED", "PACKING"),
        "PackingStarted": ("Packed", "warehouse_tasks", "WarehouseTask", "PACKING", "PACKED"),
        "Packed": ("OutboundShipmentCreated", "shipments", "Shipment", "PACKED", "CREATED"),
        "OutboundShipmentCreated": ("CarrierAssigned", "transportation", "Shipment", "CREATED", "ASSIGNED"),
        "CarrierAssigned": ("TruckLoaded", "transportation", "Shipment", "ASSIGNED", "LOADED"),
        "TruckLoaded": ("TruckDeparted", "transportation", "Shipment", "LOADED", "IN_TRANSIT"),
        "TruckDeparted": ("ArrivedAtCustomer", "transportation", "Shipment", "IN_TRANSIT", "DELIVERED"),
        "ArrivedAtCustomer": ("Delivered", "shipments", "Shipment", "IN_TRANSIT", "DELIVERED"),
        "WorkerShiftStarted": ("InventoryCycleCount", "warehouse", "Warehouse", "CREATED", "ACTIVE"),
        "DemandForecastUpdated": ("ReplenishmentTriggered", "inventory", "Inventory", "AVAILABLE", "REPLENISHMENT_REQUIRED"),
        "SeasonalDemandSpike": ("StockReserved", "inventory", "Inventory", "AVAILABLE", "RESERVED"),
        "HeavyWeatherCondition": ("DelayOccurred", "transportation", "Shipment", "IN_TRANSIT", "DELAYED"),
        "DelayOccurred": ("CheckpointReached", "transportation", "Shipment", "DELAYED", "IN_TRANSIT"),
    }

    def __init__(self, queue=None, memory_mode: bool = False):
        self.queue = queue or EventQueue(memory_mode=memory_mode)
        self.memory_mode = memory_mode or self.queue.memory_mode
        self._execution_log = {}

    @staticmethod
    def _normalize_status(value):
        return (value or "UNKNOWN").upper()

    @staticmethod
    def _normalize_datetime(value):
        if value is None:
            return get_simulation_now()
        if isinstance(value, datetime):
            dt = value
        else:
            value = str(value).strip()
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _mark_execution(self, event_id, event_type, aggregate_type, aggregate_id, correlation_id, status, error_message=None, started_time=None, completed_time=None):
        now = get_simulation_now()
        completed_dt = self._normalize_datetime(completed_time) if completed_time is not None else None
        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "correlation_id": correlation_id,
            "status": status,
            "processed_at": now,
            "error_message": error_message,
            "started_time": started_time or now.isoformat(),
            "completed_time": completed_dt.isoformat() if completed_dt is not None else None,
        }
        if self.memory_mode:
            self._execution_log[event_id] = payload
        else:
            self._write_execution_log(payload)
        return payload

    @staticmethod
    def _write_execution_log(payload):
        started_at = EventExecutor._normalize_datetime(payload.get("started_time"))
        completed_at = payload.get("completed_time")
        completed_dt = EventExecutor._normalize_datetime(completed_at) if completed_at else None
        processing_time_ms = None
        if completed_dt is not None:
            processing_time_ms = max(0, int((completed_dt - started_at).total_seconds() * 1000))
        with Database() as db:
            db.execute(
                """
                INSERT INTO event_execution_log (
                    execution_id, event_id, event_type, aggregate_type,
                    aggregate_id, correlation_id, status, started_at,
                    completed_at, processing_time_ms, error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    aggregate_type = EXCLUDED.aggregate_type,
                    aggregate_id = EXCLUDED.aggregate_id,
                    correlation_id = EXCLUDED.correlation_id,
                    status = EXCLUDED.status,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    processing_time_ms = EXCLUDED.processing_time_ms,
                    error_message = EXCLUDED.error_message
                """,
                (
                    str(uuid.uuid4()), payload["event_id"], payload["event_type"],
                    payload.get("aggregate_type") or "scheduled_events",
                    payload.get("aggregate_id") or "SIM-ROOT", payload.get("correlation_id"),
                    payload["status"], started_at, completed_dt, processing_time_ms,
                    payload.get("error_message"),
                ),
            )

    def _already_completed(self, event_id):
        if self.memory_mode:
            record = self._execution_log.get(event_id)
            if record and self._normalize_status(record.get("status")) in {"COMPLETED", "SKIPPED", "FAILED"}:
                return True
            if self.queue is not None and hasattr(self.queue, "_memory_events"):
                for row in self.queue._memory_events:
                    if str(row.get("event_id")) == str(event_id) and self._normalize_status(row.get("status")) in {"COMPLETED", "SKIPPED", "FAILED"}:
                        return True
            return False
        with Database() as db:
            row = db.fetch_one(
                """
                SELECT status
                FROM event_outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )
        return bool(row and self._normalize_status(row.get("status")) in {"COMPLETED", "SKIPPED"})

    def _validate_state_transition(self, event_row, payload):
        if payload.get("domain_signal") or (payload.get("payload") or {}).get("domain_signal"):
            return
        if event_row.get("event_type") == "PurchaseOrderCreated":
            event_payload = dict(payload.get("payload") or payload)
            purchase_order = dict(event_payload.get("purchaseOrder") or {})
            payload_id = purchase_order.get("poId") or event_payload.get("po_id")
            event_id = event_row.get("aggregate_id")
            if event_id and payload_id and str(event_id) != str(payload_id):
                raise InvalidStateTransition("PurchaseOrder ID mismatch between event and payload")

        entity_type = payload.get("entity_type") or event_row.get("aggregate_type")
        current_state = payload.get("current_state") or payload.get("state")
        next_state = payload.get("next_state") or infer_next_state(event_row["event_type"])

        if current_state and next_state:
            if not validate_transition(current_state, next_state, entity_type):
                raise InvalidStateTransition(
                    f"Invalid state transition for {event_row['event_type']}: {current_state} -> {next_state}"
                )

    def _build_handler_context(self, event_row):
        payload = dict(event_row.get("payload") or {})
        return {
            "event_id": str(event_row["event_id"]),
            "event_type": event_row["event_type"],
            "aggregate_type": event_row.get("aggregate_type"),
            "aggregate_id": event_row.get("aggregate_id"),
            "correlation_id": event_row.get("correlation_id"),
            "scheduled_time": payload.get("event_scheduled_time"),
            "current_state": payload.get("current_state") or payload.get("state"),
            "next_state": payload.get("next_state") or infer_next_state(event_row["event_type"]),
            "entity_type": payload.get("entity_type") or infer_entity_type(event_row["event_type"]) or event_row.get("aggregate_type"),
            "payload": payload,
        }

    def _compute_retry_interval(self, retry_count):
        backoff_minutes = min(5 * (2 ** max(retry_count - 1, 0)), 60)
        return timedelta(minutes=backoff_minutes)

    @staticmethod
    def _is_valid_id(value):
        if value is None:
            return False
        value = str(value).strip()
        return value not in {"", "SIM-ROOT", "None", "null"}

    @staticmethod
    def _extract_nested_id(container, field_name):
        if isinstance(container, dict):
            value = container.get(field_name)
            if value not in (None, "", "SIM-ROOT", "None", "null"):
                return str(value)
            for nested_key in ("inventory", "goods_received", "receiving_task", "picking", "shipment", "order", "po", "tasks", "items"):
                nested = container.get(nested_key)
                if isinstance(nested, dict):
                    found = EventExecutor._extract_nested_id(nested, field_name)
                    if found is not None:
                        return found
                elif isinstance(nested, list):
                    for item in nested:
                        found = EventExecutor._extract_nested_id(item, field_name)
                        if found is not None:
                            return found
        elif isinstance(container, list):
            for item in container:
                found = EventExecutor._extract_nested_id(item, field_name)
                if found is not None:
                    return found
        return None

    def _resolve_real_aggregate_id(self, event_type, payload, fallback=None, aggregate_type=None):
        config = self.NEXT_EVENT_SEQUENCE.get(event_type) or {}
        aggregate_type = aggregate_type or payload.get("aggregate_type") or event_type or config.get("aggregate_type") or (fallback or "")
        preferred_fields = ["aggregate_id"]

        if aggregate_type in {"inventory", "inventory_locations"}:
            preferred_fields = ["inventory_id", "aggregate_id", "task_id", "shipment_id", "po_id", "order_id"]
        elif aggregate_type == "warehouse_tasks":
            preferred_fields = ["task_id", "aggregate_id", "order_id", "shipment_id", "po_id", "inventory_id"]
        elif aggregate_type == "shipments":
            preferred_fields = ["shipment_id", "aggregate_id", "po_id", "task_id", "order_id", "inventory_id"]
        elif aggregate_type == "purchase_orders":
            preferred_fields = ["po_id", "aggregate_id", "shipment_id", "task_id", "order_id", "inventory_id"]
        elif aggregate_type == "orders":
            preferred_fields = ["order_id", "aggregate_id", "shipment_id", "task_id", "po_id", "inventory_id"]
        else:
            preferred_fields = ["po_id", "shipment_id", "task_id", "order_id", "inventory_id", "aggregate_id"]

        for field_name in preferred_fields:
            value = payload.get(field_name)
            if self._is_valid_id(value):
                return str(value)
            nested_value = self._extract_nested_id(payload, field_name)
            if self._is_valid_id(nested_value):
                return str(nested_value)

        if self._is_valid_id(fallback):
            return str(fallback)
        if self._is_valid_id(payload.get("aggregate_id")):
            return str(payload.get("aggregate_id"))
        return "SIM-ROOT"

    def _build_default_next_event(self, event_row, payload):
        event_type = str(event_row.get("event_type") or payload.get("event_type") or "")
        config = self.NEXT_EVENT_SEQUENCE.get(event_type)
        if payload.get("domain_signal") and event_type in self.DOMAIN_NEXT_EVENT_SEQUENCE:
            next_type, aggregate_type, entity_type, current_state, next_state = self.DOMAIN_NEXT_EVENT_SEQUENCE[event_type]
            config = {
                "event_type": next_type,
                "aggregate_type": aggregate_type,
                "entity_type": entity_type,
                "current_state": current_state,
                "next_state": next_state,
                "priority": "MEDIUM",
            }
        if config is None:
            return None

        if config["event_type"] == "PackingTaskCreated" and self._is_valid_id(payload.get("order_id")):
            aggregate_id = str(payload["order_id"])
        else:
            aggregate_id = self._resolve_real_aggregate_id(event_type, payload, event_row.get("aggregate_id") or payload.get("aggregate_id"), aggregate_type=config.get("aggregate_type"))

        source_correlation = event_row.get("correlation_id") or payload.get("correlation_id")
        reference_time = self._normalize_datetime(event_row.get("scheduled_time") or payload.get("event_scheduled_time") or get_simulation_now())
        scheduled_time = get_future_event_time(config["event_type"], base_time=reference_time)

        next_payload = {
            "event_type": config["event_type"],
            "aggregate_type": config["aggregate_type"],
            "aggregate_id": str(aggregate_id),
            "correlation_id": str(source_correlation or uuid.uuid4()),
            "entity_type": config["entity_type"],
            "current_state": config["current_state"],
            "next_state": config["next_state"],
            "priority": str(config.get("priority") or "MEDIUM").upper(),
            "event_scheduled_time": scheduled_time.isoformat(),
            "status": "PENDING",
        }

        if payload.get("po_id") is not None:
            next_payload["po_id"] = payload["po_id"]
        if payload.get("shipment_id") is not None:
            next_payload["shipment_id"] = payload["shipment_id"]
        if payload.get("task_id") is not None:
            next_payload["task_id"] = payload["task_id"]
        if payload.get("order_id") is not None:
            next_payload["order_id"] = payload["order_id"]
        if payload.get("inventory_id") is not None:
            next_payload["inventory_id"] = payload["inventory_id"]
        if payload.get("domain_signal"):
            next_payload["domain_signal"] = True
            next_payload["domain"] = payload.get("domain")

        return {
            "event_type": config["event_type"],
            "event_name": config["event_type"],
            "aggregate_type": config["aggregate_type"],
            "aggregate_id": str(aggregate_id),
            "correlation_id": str(source_correlation or uuid.uuid4()),
            "payload": next_payload,
            "scheduled_time": scheduled_time.isoformat(),
            "priority": str(config.get("priority") or "MEDIUM").upper(),
        }

    def execute(self, event_row, registry=None):
        if self.memory_mode:
            return self._execute(event_row, registry=registry)
        with Database():
            return self._execute(event_row, registry=registry)

    def _execute(self, event_row, registry=None):
        event_id = str(event_row["event_id"])
        event_type = event_row["event_type"]
        aggregate_type = event_row.get("aggregate_type")
        aggregate_id = event_row.get("aggregate_id")
        correlation_id = event_row.get("correlation_id")
        payload = dict(event_row.get("payload") or {})
        event_row["payload"] = payload

        if aggregate_id in {None, "", "None", "null"}:
            aggregate_id = payload.get("aggregate_id") or payload.get("po_id") or payload.get("shipment_id") or payload.get("task_id") or payload.get("order_id") or payload.get("inventory_id") or "SIM-ROOT"
            event_row["aggregate_id"] = aggregate_id
            payload["aggregate_id"] = aggregate_id

        if correlation_id in {None, "", "None", "null"}:
            correlation_id = str(uuid.uuid4())
            event_row["correlation_id"] = correlation_id
            payload["correlation_id"] = correlation_id

        if self._already_completed(event_id):
            skipped = self.queue.mark_event_status(event_id, "SKIPPED", processed_at=get_simulation_now())
            return {
                "event_id": event_id,
                "event_type": event_type,
                "status": "SKIPPED",
                "reason": "already processed",
                "history_entry": skipped,
            }

        handler_registry = registry or EVENT_REGISTRY
        handler = handler_registry.get(event_type)
        if handler is None and payload.get("domain_signal"):
            handler = handler_registry.get("__domain_signal__")
        if handler is None:
            raise KeyError(f"No handler registered for {event_type}")

        context = self._build_handler_context(event_row)
        try:
            self._validate_state_transition(event_row, context)
        except InvalidStateTransition as exc:
            event_row["status"] = "FAILED"
            self.queue.mark_event_status(event_id, "FAILED", processed_at=get_simulation_now(), error_message=str(exc))
            self._mark_execution(event_id, event_type, aggregate_type, aggregate_id, correlation_id, "STARTED", started_time=get_simulation_now().isoformat())
            self._mark_execution(event_id, event_type, aggregate_type, aggregate_id, correlation_id, "FAILED", str(exc))
            return {"event_id": event_id, "event_type": event_type, "status": "FAILED", "error": str(exc)}

        created_at = self._normalize_datetime(event_row.get("event_created_time") or event_row.get("payload", {}).get("event_created_time") or get_simulation_now())
        started_at = created_at + timedelta(seconds=EVENT_PROCESSING_LATENCY_SECONDS)
        payload["event_started_time"] = started_at.isoformat()
        payload["status"] = "RUNNING"
        event_row["event_started_time"] = started_at.isoformat()
        event_row["payload"] = payload
        self._mark_execution(event_id, event_type, aggregate_type, aggregate_id, correlation_id, "STARTED", started_time=started_at.isoformat())

        try:
            signature = inspect.signature(handler)
            if len(signature.parameters) == 0:
                result = handler()
            else:
                result = handler(context)
        except TypeError:
            result = handler()
        except Exception as exc:
            retry_count = int(payload.get("retry_count", 0)) + 1
            max_retry_count = int(payload.get("max_retry_count", 3))
            payload["last_error"] = str(exc)
            payload["retry_count"] = retry_count
            error_text = str(exc)
            is_idempotent_noop = any(
                token in error_text.lower()
                for token in (
                    "already exists",
                    "already completed",
                    "already received",
                    "already processed",
                    "no started",
                    "no arrived",
                    "no created",
                    "invalid input syntax for type bigint",
                )
            )

            if is_idempotent_noop:
                payload["status"] = "SKIPPED"
                event_row["status"] = "SKIPPED"
                self.queue.mark_event_status(event_id, "SKIPPED", processed_at=get_simulation_now(), error_message=error_text, retry_count=retry_count, priority=payload.get("priority", "MEDIUM"))
                self._mark_execution(event_id, event_type, aggregate_type, aggregate_id, correlation_id, "SKIPPED", str(exc), started_time=payload.get("event_started_time"), completed_time=get_simulation_now().isoformat())
                return {
                    "event_id": event_id,
                    "event_type": event_type,
                    "status": "SKIPPED",
                    "retry_count": retry_count,
                    "max_retry_count": max_retry_count,
                    "error": error_text,
                }

            if retry_count <= max_retry_count:
                retry_base_time = self._normalize_datetime(
                    event_row.get("scheduled_time")
                    or payload.get("event_scheduled_time")
                    or get_simulation_now()
                )
                next_retry_time = retry_base_time + self._compute_retry_interval(retry_count)
                payload["next_retry_time"] = next_retry_time.isoformat()
                payload["status"] = "RETRY_SCHEDULED"
                event_row["payload"] = payload
                event_row["status"] = "RETRY_SCHEDULED"
                event_row["next_retry_time"] = next_retry_time.isoformat()
                self.queue.mark_event_status(event_id, "RETRY_SCHEDULED", processed_at=get_simulation_now(), error_message=str(exc), retry_count=retry_count, next_retry_time=next_retry_time, priority=payload.get("priority", "MEDIUM"))
                return {
                    "event_id": event_id,
                    "event_type": event_type,
                    "status": "RETRY_SCHEDULED",
                    "retry_count": retry_count,
                    "max_retry_count": max_retry_count,
                    "next_retry_time": next_retry_time.isoformat(),
                    "error": str(exc),
                }

            payload["status"] = "FAILED"
            event_row["status"] = "FAILED"
            self.queue.mark_event_status(event_id, "FAILED", processed_at=get_simulation_now(), error_message=str(exc), retry_count=retry_count, priority=payload.get("priority", "MEDIUM"))
            self._mark_execution(event_id, event_type, aggregate_type, aggregate_id, correlation_id, "FAILED", str(exc), started_time=payload.get("event_started_time"), completed_time=get_simulation_now().isoformat())
            return {
                "event_id": event_id,
                "event_type": event_type,
                "status": "FAILED",
                "retry_count": retry_count,
                "max_retry_count": max_retry_count,
                "error": str(exc),
            }

        standardized = result if isinstance(result, dict) else {"status": "SUCCESS", "created_events": []}
        standardized.setdefault("status", "SUCCESS")
        standardized.setdefault("created_events", [])

        if isinstance(result, dict):
            for key, value in result.items():
                if key == "created_events":
                    continue
                if value is None:
                    continue
                if key in {"po_id", "shipment_id", "task_id", "order_id", "inventory_id", "allocation_id"}:
                    payload[key] = str(value)
                elif key == "inventory" and isinstance(value, list):
                    payload["inventory"] = value
                    for item in value:
                        if isinstance(item, dict):
                            for nested_key in ("inventory_id", "task_id", "shipment_id", "po_id", "order_id"):
                                if nested_key in item and item[nested_key] not in (None, "", "SIM-ROOT", "None", "null"):
                                    payload[nested_key] = str(item[nested_key])
                                    break
                elif key == "goods_received" and isinstance(value, dict):
                    payload["goods_received"] = value
                    for nested_key in ("inventory_id", "task_id", "shipment_id", "po_id", "order_id"):
                        if nested_key in value and value[nested_key] not in (None, "", "SIM-ROOT", "None", "null"):
                            payload[nested_key] = str(value[nested_key])
                            break

        if event_type == "SupplierShipmentCreated":
            shipment_id = standardized.get("shipment_id") or payload.get("shipment_id") or result.get("shipment_id")
            if shipment_id:
                payload["shipment_id"] = str(shipment_id)
                payload["po_id"] = payload.get("po_id") or payload.get("aggregate_id") or (event_row.get("payload") or {}).get("po_id") or event_row.get("aggregate_id")
                payload["aggregate_id"] = str(shipment_id)
                event_row["aggregate_id"] = str(shipment_id)
                event_row["payload"] = payload
                aggregate_id = str(shipment_id)
                self.queue.update_event_aggregate_id(event_id, str(shipment_id), payload)

        real_aggregate_id = self._resolve_real_aggregate_id(event_type, payload, event_row.get("aggregate_id") or payload.get("aggregate_id"), aggregate_type=payload.get("aggregate_type") or event_row.get("aggregate_type"))
        if real_aggregate_id and str(real_aggregate_id) not in {"SIM-ROOT", "None", "null"} and str(real_aggregate_id) != str(event_row.get("aggregate_id") or ""):
            event_row["aggregate_id"] = str(real_aggregate_id)
            payload["aggregate_id"] = str(real_aggregate_id)
            aggregate_id = str(real_aggregate_id)
            self.queue.update_event_aggregate_id(event_id, str(real_aggregate_id), payload)

        if not standardized.get("created_events"):
            default_event = self._build_default_next_event(event_row, payload)
            if default_event is not None:
                standardized["created_events"] = [default_event]

        created_events = []
        canonical_aggregate_id = str(payload.get("aggregate_id") or aggregate_id or event_row.get("aggregate_id") or "SIM-ROOT")
        for item in standardized.get("created_events") or []:
            if not isinstance(item, dict):
                continue
            explicit_aggregate_id = item.get("aggregate_id") or item.get("payload", {}).get("aggregate_id") or payload.get("aggregate_id") or aggregate_id or canonical_aggregate_id
            normalized = {
                "event_name": item.get("event_name") or item.get("event_type") or event_type,
                "event_type": item.get("event_type") or item.get("event_name") or event_type,
                "aggregate_type": item.get("aggregate_type") or aggregate_type or "scheduled_events",
                "aggregate_id": str(explicit_aggregate_id or canonical_aggregate_id or "SIM-ROOT"),
                "correlation_id": str(item.get("correlation_id") or correlation_id or uuid.uuid4()),
                "payload": dict(item.get("payload") or {}),
                "priority": str(item.get("priority") or payload.get("priority") or "MEDIUM").upper(),
                "execute_at": str(item.get("execute_at") or item.get("scheduled_time") or get_simulation_now().isoformat()),
                "scheduled_time": str(item.get("scheduled_time") or item.get("execute_at") or get_simulation_now().isoformat()),
            }
            normalized["payload"].setdefault("correlation_id", normalized["correlation_id"])
            normalized["payload"]["aggregate_id"] = normalized["aggregate_id"]
            normalized["payload"]["aggregate_type"] = normalized["aggregate_type"]
            if "shipment_id" in payload and normalized["aggregate_type"] == "shipments":
                normalized["payload"].setdefault("shipment_id", str(payload["shipment_id"]))
            if "po_id" in payload and normalized["aggregate_type"] in {"shipments", "purchase_orders"}:
                normalized["payload"].setdefault("po_id", str(payload["po_id"]))
            created_events.append(normalized)

        standardized["created_events"] = created_events

        completed_at = started_at + timedelta(seconds=EVENT_PROCESSING_LATENCY_SECONDS)
        payload["event_completed_time"] = completed_at.isoformat()
        payload["event_processed_time"] = completed_at.isoformat()
        payload["status"] = "COMPLETED"
        payload["retry_count"] = int(payload.get("retry_count", 0))
        payload["priority"] = payload.get("priority", "MEDIUM")
        payload["correlation_id"] = str(correlation_id)
        event_row["payload"] = payload
        event_row["event_completed_time"] = completed_at.isoformat()
        event_row["status"] = "COMPLETED"

        self._mark_execution(event_id, event_type, aggregate_type, aggregate_id, correlation_id, "COMPLETED", started_time=payload.get("event_started_time"), completed_time=completed_at.isoformat())
        history_entry = self.queue.mark_event_status(event_id, "COMPLETED", processed_at=completed_at, priority=payload.get("priority"))
        return {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "correlation_id": correlation_id,
            "status": "COMPLETED",
            "history_entry": history_entry,
            "result": standardized,
        }
