import importlib
import inspect
import uuid


def _resolve_generator(generator_ref):
    if callable(generator_ref):
        return generator_ref
    if isinstance(generator_ref, str):
        try:
            module_name, attr_name = generator_ref.rsplit(".", 1)
            module = importlib.import_module(module_name)
            return getattr(module, attr_name)
        except (ImportError, AttributeError, ValueError):
            def _empty_generator():
                return {"created_events": []}
            return _empty_generator
    raise TypeError(f"Unsupported generator reference: {generator_ref!r}")


def _coerce_correlation_id(event_context, result=None):
    context = dict(event_context or {})
    value = context.get("correlation_id") or (dict(result or {}).get("correlation_id"))
    return str(value) if value is not None else str(uuid.uuid4())


def _coerce_id_value(context, payload, field_name):
    sources = [payload, context]
    for source in sources:
        if not isinstance(source, dict):
            continue
        value = source.get(field_name)
        if value not in (None, "", "SIM-ROOT", "None", "null"):
            return value
        nested = source.get("inventory")
        if isinstance(nested, dict) and nested.get(field_name) not in (None, "", "SIM-ROOT", "None", "null"):
            return nested.get(field_name)
        if isinstance(nested, list):
            for item in nested:
                if isinstance(item, dict):
                    value = item.get(field_name)
                    if value not in (None, "", "SIM-ROOT", "None", "null"):
                        return value
    return None


def _coerce_aggregate_id(event_context, result=None, aggregate_id_field="po_id"):
    context = dict(event_context or {})
    payload = dict(result or {})
    preferred_fields = [aggregate_id_field, "aggregate_id", "po_id", "shipment_id", "order_id", "task_id", "inventory_id", "product_id"]
    for field in preferred_fields:
        value = _coerce_id_value(context, payload, field)
        if value is not None:
            return str(value)
    return "SIM-ROOT"


def _normalize_created_event(event, default_event_type=None, aggregate_type="scheduled_events", correlation_id=None, aggregate_id=None):
    if not isinstance(event, dict):
        return None

    item = dict(event)
    event_type = item.get("event_type") or default_event_type or "GeneratedEvent"
    scheduled_time = item.get("scheduled_time") or item.get("execute_at") or item.get("scheduled_at") or item.get("event_scheduled_time") or item.get("event_time")

    normalized = {
        "event_type": event_type,
        "aggregate_type": item.get("aggregate_type") or aggregate_type,
        "aggregate_id": str(item.get("aggregate_id") or aggregate_id or "SIM-ROOT"),
        "correlation_id": str(item.get("correlation_id") or correlation_id or uuid.uuid4()),
        "scheduled_time": scheduled_time,
        "payload": dict(item.get("payload") or {}),
    }
    if normalized["scheduled_time"] is None:
        normalized["scheduled_time"] = item.get("scheduled_time") or item.get("execute_at")
    if normalized["payload"] and "correlation_id" not in normalized["payload"]:
        normalized["payload"]["correlation_id"] = normalized["correlation_id"]
    if normalized["payload"] and "aggregate_id" not in normalized["payload"]:
        normalized["payload"]["aggregate_id"] = normalized["aggregate_id"]
    if normalized["payload"] and "aggregate_type" not in normalized["payload"]:
        normalized["payload"]["aggregate_type"] = normalized["aggregate_type"]
    return normalized


def build_handler(generator_ref, event_type=None, aggregate_type="scheduled_events", aggregate_id_field="po_id"):
    def handle(event_context=None):
        context = dict(event_context or {})
        if event_type == "PurchaseOrderCreated":
            event_payload = dict(context.get("payload") or {})
            purchase_order = dict(event_payload.get("purchaseOrder") or {})
            event_id = context.get("aggregate_id")
            payload_id = purchase_order.get("poId") or event_payload.get("po_id")
            if event_id and payload_id and str(event_id) != str(payload_id):
                raise ValueError("PurchaseOrder ID mismatch between event and payload")
            resolved_id = event_id or payload_id
            if resolved_id:
                context["aggregate_id"] = str(resolved_id)
                event_payload["po_id"] = str(resolved_id)
                context["payload"] = event_payload
        payload = dict(context.get("payload") or {})
        if aggregate_id_field and payload.get(aggregate_id_field) not in (None, "", "SIM-ROOT", "None", "null"):
            context[aggregate_id_field] = payload[aggregate_id_field]
            context["aggregate_id"] = payload[aggregate_id_field]
        elif aggregate_id_field and context.get(aggregate_id_field) not in (None, "", "SIM-ROOT", "None", "null"):
            payload.setdefault(aggregate_id_field, context[aggregate_id_field])

        generator_fn = _resolve_generator(generator_ref)
        if callable(generator_fn):
            params = inspect.signature(generator_fn).parameters
            if not params:
                result = generator_fn()
            else:
                keyword_args = {}
                for name, param in params.items():
                    if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                        continue
                    if name == "event_context" or name == "context":
                        keyword_args[name] = context
                    elif name in context and context.get(name) is not None:
                        keyword_args[name] = context.get(name)
                    elif name == "po_id":
                        value = _coerce_id_value(context, payload, "po_id") or context.get("aggregate_id")
                        if value is not None:
                            keyword_args[name] = value
                    elif name == "shipment_id":
                        value = _coerce_id_value(context, payload, "shipment_id") or context.get("aggregate_id")
                        if value is not None:
                            keyword_args[name] = value
                    elif name == "order_id":
                        value = _coerce_id_value(context, payload, "order_id") or context.get("aggregate_id")
                        if value is not None:
                            keyword_args[name] = value
                    elif name == "task_id":
                        value = _coerce_id_value(context, payload, "task_id") or context.get("aggregate_id")
                        if value is not None:
                            keyword_args[name] = value
                    elif name == "inventory_id":
                        value = _coerce_id_value(context, payload, "inventory_id") or context.get("aggregate_id")
                        if value is not None:
                            keyword_args[name] = value
                    elif name == "allocation_id":
                        value = _coerce_id_value(context, payload, "allocation_id") or context.get("aggregate_id")
                        if value is not None:
                            keyword_args[name] = value
                    elif name == "count":
                        if "count" in context:
                            keyword_args[name] = context["count"]
                        else:
                            keyword_args[name] = 1
                if keyword_args:
                    result = generator_fn(**keyword_args)
                else:
                    result = generator_fn(context)
        else:
            result = {}
        payload = dict(result or {})
        correlation_id = _coerce_correlation_id(context, payload)
        aggregate_id = _coerce_aggregate_id(context, payload, aggregate_id_field)

        raw_created = payload.get("created_events")
        if isinstance(raw_created, dict):
            raw_created = [raw_created]
        if not isinstance(raw_created, list):
            raw_created = []

        created_events = []
        for item in raw_created:
            normalized = _normalize_created_event(
                item,
                default_event_type=item.get("event_type") if isinstance(item, dict) else event_type,
                aggregate_type=item.get("aggregate_type") if isinstance(item, dict) else aggregate_type,
                correlation_id=item.get("correlation_id") if isinstance(item, dict) else correlation_id,
                aggregate_id=item.get("aggregate_id") if isinstance(item, dict) else aggregate_id,
            )
            if normalized is not None:
                created_events.append(normalized)

        return {
            "status": "SUCCESS",
            "created_events": created_events,
            "result": payload,
        }

    return handle
