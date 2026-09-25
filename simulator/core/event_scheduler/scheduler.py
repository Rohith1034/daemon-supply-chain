from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_queue import EventQueue
from core.event_scheduler.event_registry import EVENT_REGISTRY
from core.simulation_clock import get_simulation_now


class EventScheduler:
    """Dispatch ready events in a single Airflow-triggered batch."""

    def __init__(self, queue=None, executor=None, registry=None):
        self.queue = queue or EventQueue()
        self.executor = executor or EventExecutor(queue=self.queue)
        self.registry = registry or EVENT_REGISTRY

    def dispatch_ready_events(self, simulation_time=None, from_time=None):
        ready_events = self.queue.list_ready_events(simulation_time, from_time=from_time)
        processed = []

        for event in ready_events:
            result = self.executor.execute(event, registry=self.registry)
            processed.append(result)

        return processed

    def run_batch(self, simulation_time=None, from_time=None):
        scheduled_time = simulation_time or get_simulation_now()
        return self.dispatch_ready_events(scheduled_time, from_time=from_time)
