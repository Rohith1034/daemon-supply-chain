from __future__ import annotations

from time import perf_counter

METRICS = {
    "events_created_total": 0,
    "events_processed_total": 0,
    "events_failed_total": 0,
    "orders_created": 0,
    "orders_delivered": 0,
    "inventory_received_qty": 0,
    "inventory_putaway_qty": 0,
    "event_processing_latency": [],
}


def increment_counter(name, value=1):
    if name not in METRICS:
        METRICS[name] = 0
    METRICS[name] += value
    return METRICS[name]


def record_latency(latency_seconds):
    METRICS["event_processing_latency"].append(float(latency_seconds))
    return latency_seconds


def record_business_metric(name, value=1):
    increment_counter(name, value)
    return METRICS.get(name, 0)


def measure_execution(func, *args, **kwargs):
    started = perf_counter()
    try:
        result = func(*args, **kwargs)
        return result
    finally:
        record_latency(perf_counter() - started)
