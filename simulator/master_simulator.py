import inspect
import os
import sys
import uuid
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.checkpointing import SimulationCheckpoint
from core.contracts import is_event_allowed_for_window, normalize_run_window, validate_lms_sequence
from core.event_scheduler.event_queue import EventQueue
from core.event_scheduler.event_registry import EVENT_REGISTRY
from core.event_scheduler.scheduler import EventScheduler
from core.event_timing import get_future_event_time
from core.simulation_clock import get_simulation_now


class SimulationTimeError(ValueError):
    pass


def _as_utc(value=None):
    if value is None:
        value = get_simulation_now()
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _wrap_handler(handler):
    def adapter(context=None):
        context = dict(context or {})
        try:
            signature = inspect.signature(handler)
            if len(signature.parameters) == 0:
                result = handler()
            else:
                result = handler(context)
        except TypeError:
            try:
                result = handler(context)
            except TypeError:
                result = handler()
        if result is None:
            return {"status": "SUCCESS", "created_events": []}
        if not isinstance(result, dict):
            return {"status": "SUCCESS", "created_events": []}
        normalized = dict(result)
        normalized.setdefault("status", "SUCCESS")
        normalized.setdefault("created_events", [])
        return normalized

    return adapter


def _resolve_canonical_aggregate_id(event_spec, payload):
    aggregate_type = event_spec.get("aggregate_type") or "scheduled_events"
    explicit = event_spec.get("aggregate_id")
    candidates = [
        "aggregate_id",
        "shipment_id",
        "task_id",
        "inventory_id",
        "order_id",
        "po_id",
    ]
    if aggregate_type in {"shipments", "shipment_transportation", "shipment_loading_events", "shipment_tracking", "shipment_checkpoints"}:
        candidates = ["shipment_id", "aggregate_id", "task_id", "inventory_id", "order_id", "po_id"]
    elif aggregate_type in {"warehouse_tasks"}:
        candidates = ["task_id", "shipment_id", "aggregate_id", "inventory_id", "order_id", "po_id"]
    elif aggregate_type in {"inventory", "inventory_locations", "inventory_allocations", "inventory_reservations"}:
        candidates = ["inventory_id", "task_id", "shipment_id", "aggregate_id", "order_id", "po_id"]
    elif aggregate_type in {"purchase_orders"}:
        candidates = ["po_id", "aggregate_id", "shipment_id", "task_id", "inventory_id", "order_id"]
    elif aggregate_type in {"orders"}:
        candidates = ["order_id", "aggregate_id", "shipment_id", "task_id", "inventory_id", "po_id"]

    for field in candidates:
        value = payload.get(field)
        if value not in (None, "", "SIM-ROOT", "None", "null"):
            return str(value)
    if explicit not in (None, "", "SIM-ROOT", "None", "null"):
        return str(explicit)
    return "SIM-ROOT"


def _coerce_future_event(event_spec, default_base_time=None):
    if not isinstance(event_spec, dict):
        return None

    event_type = event_spec.get("event_type")
    if not event_type:
        return None

    base_time = _as_utc(default_base_time or event_spec.get("scheduled_time") or get_simulation_now())
    scheduled_time = _as_utc(event_spec.get("scheduled_time") or base_time)
    if scheduled_time <= base_time:
        scheduled_time = get_future_event_time(event_type, base_time=base_time)
    payload = dict(event_spec.get("payload") or {})
    payload.setdefault("correlation_id", event_spec.get("correlation_id") or str(uuid.uuid4()))
    payload.setdefault("event_scheduled_time", scheduled_time.isoformat())
    payload.setdefault("event_type", event_type)
    payload.setdefault("priority", event_spec.get("priority") or "MEDIUM")
    aggregate_id = _resolve_canonical_aggregate_id(event_spec, payload)

    return {
        "event_type": event_type,
        "aggregate_type": event_spec.get("aggregate_type") or "scheduled_events",
        "aggregate_id": aggregate_id,
        "correlation_id": str(event_spec.get("correlation_id") or payload.get("correlation_id") or uuid.uuid4()),
        "payload": payload,
        "scheduled_time": scheduled_time.isoformat(),
        "priority": str(event_spec.get("priority") or payload.get("priority") or "MEDIUM").upper(),
    }


def _schedule_returned_events(queue, created_events, base_time=None):
    queue = queue or EventQueue()
    scheduled = []
    for event_spec in created_events or []:
        normalized = _coerce_future_event(event_spec, default_base_time=base_time)
        if normalized is None:
            continue

        event_id = queue.insert_future_event(
            event_type=normalized["event_type"],
            aggregate_type=normalized["aggregate_type"],
            aggregate_id=normalized["aggregate_id"],
            correlation_id=normalized["correlation_id"],
            payload=normalized["payload"],
            scheduled_time=normalized["scheduled_time"],
            priority=normalized["priority"],
        )
        scheduled.append({
            "event_id": event_id,
            "event_type": normalized["event_type"],
            "aggregate_type": normalized["aggregate_type"],
            "aggregate_id": normalized["aggregate_id"],
            "correlation_id": normalized["correlation_id"],
            "scheduled_time": normalized["scheduled_time"],
        })
    return scheduled


def _build_registry(registry=None):
    base_registry = dict(EVENT_REGISTRY if registry is None else registry)
    return {name: _wrap_handler(handler) for name, handler in base_registry.items()}


def run_simulation_cycle(simulation_time=None, queue=None, checkpoint_path=None, registry=None, run_window=None):
    queue = queue or EventQueue()
    scheduler = EventScheduler(queue=queue, registry=_build_registry(registry))
    checkpoint = SimulationCheckpoint(file_path=checkpoint_path) if checkpoint_path else SimulationCheckpoint()
    normalized_window = normalize_run_window(run_window)
    current_time = _as_utc(simulation_time or get_simulation_now())
    last_checkpoint = checkpoint.read()
    from_time = last_checkpoint.isoformat() if last_checkpoint else None

    ready = queue.list_ready_events(current_time.isoformat(), from_time=from_time)
    filtered_ready = []
    for event in ready:
        event_type = (event.get("event_type") or event.get("event_name") or "")
        if is_event_allowed_for_window(event_type, normalized_window):
            filtered_ready.append(event)
        elif event_type in {"PunchIn", "PunchOut", "ShiftStart", "ShiftEnd", "BreakStart", "BreakEnd", "WorkerCheckIn", "WorkerCheckOut"}:
            filtered_ready.append(event)
    processed = []
    newly_scheduled = []

    for event in filtered_ready:
        event_type = event.get("event_type") or event.get("event_name") or ""
        if event_type in {"PunchIn", "PunchOut", "ShiftStart", "ShiftEnd", "BreakStart", "BreakEnd", "WorkerCheckIn", "WorkerCheckOut"}:
            sequence = []
            for candidate in queue._memory_events if hasattr(queue, "_memory_events") else []:
                if str(candidate.get("event_type") or candidate.get("event_name") or "") in {"PunchIn", "PunchOut", "ShiftStart", "ShiftEnd", "BreakStart", "BreakEnd", "WorkerCheckIn", "WorkerCheckOut"}:
                    sequence.append(str(candidate.get("event_type") or candidate.get("event_name") or ""))
            if not validate_lms_sequence(sequence + [event_type]):
                continue
        result = scheduler.executor.execute(event, registry=scheduler.registry)
        processed.append(result)
        created_events = result.get("result", {}).get("created_events") if isinstance(result.get("result"), dict) else []
        if created_events:
            scheduleable = []
            for created in created_events:
                created_type = created.get("event_type") or created.get("event_name") or ""
                if is_event_allowed_for_window(created_type, normalized_window) or created_type in {"PunchIn", "PunchOut", "ShiftStart", "ShiftEnd", "BreakStart", "BreakEnd", "WorkerCheckIn", "WorkerCheckOut"}:
                    scheduleable.append(created)
            newly_scheduled.extend(_schedule_returned_events(queue, scheduleable, base_time=current_time))

    has_failure = any(
        item.get("status") in {"FAILED", "RETRY_SCHEDULED"}
        for item in processed
        if isinstance(item, dict)
    )
    if not has_failure:
        checkpoint.advance_to(current_time)

    return {
        "simulation_time": current_time.isoformat(),
        "last_checkpoint": checkpoint.read().isoformat() if checkpoint.read() else None,
        "processed_events": processed,
        "scheduled_events": newly_scheduled,
        "ready_events": queue.list_ready_events(current_time.isoformat(), from_time=from_time),
        "queue_size": queue.queue_size,
        "run_window": normalized_window,
    }


if __name__ == "__main__":
    print(run_simulation_cycle())
