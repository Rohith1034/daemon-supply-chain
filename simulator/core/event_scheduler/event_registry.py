from event_handlers import EVENT_HANDLER_REGISTRY

EVENT_REGISTRY = dict(EVENT_HANDLER_REGISTRY)


def get_event_handler(event_type: str):
    try:
        return EVENT_REGISTRY[event_type]
    except KeyError as exc:
        raise KeyError(f"No scheduler handler registered for event: {event_type}") from exc
