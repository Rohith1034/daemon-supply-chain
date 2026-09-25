from datetime import timedelta
import uuid

from core.event_scheduler.event_queue import EventQueue
from core.simulation_clock import get_simulation_now


class SimulationTimeError(ValueError):
    pass


ROOT_EVENTS = (
    ("CustomerOrderCreated", "orders", "ORDER", "outbound"),
    ("WorkerShiftStarted", "warehouse_workers", "WORKER", "warehouse"),
    ("DemandForecastUpdated", "forecasts", "FORECAST", "planning"),
    ("SeasonalDemandSpike", "demand_signals", "SEASON", "planning"),
    ("HeavyWeatherCondition", "weather", "WEATHER", "transportation"),
)


def seed_multi_domain_events(queue=None, start_time=None):
    queue = queue or EventQueue(memory_mode=False)
    start_time = start_time or get_simulation_now()
    suffix = start_time.strftime("%Y%m%d%H%M%S")
    seeded = []
    for offset, (event_type, aggregate_type, prefix, domain) in enumerate(ROOT_EVENTS, start=1):
        aggregate_id = f"{prefix}-{suffix}"
        scheduled_time = start_time + timedelta(minutes=offset * 2)
        if scheduled_time <= start_time:
            raise SimulationTimeError("event_scheduled_time must be greater than current_simulation_time")
        event_id = queue.insert_future_event(
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            correlation_id=str(uuid.uuid4()),
            payload={
                "domain": domain,
                "domain_signal": True,
                "event_type": event_type,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "correlation_id": str(uuid.uuid4()),
                "event_created_time": start_time.isoformat(),
                "event_scheduled_time": scheduled_time.isoformat(),
                    "current_state": "CREATED",
                    "next_state": "CREATED",
                    "priority": "MEDIUM",
            },
            scheduled_time=scheduled_time,
            priority="MEDIUM",
        )
        seeded.append({"event_id": event_id, "event_type": event_type, "domain": domain, "scheduled_time": scheduled_time.isoformat()})
    return seeded