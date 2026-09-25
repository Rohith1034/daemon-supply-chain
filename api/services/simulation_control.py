from __future__ import annotations

import json
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SIMULATOR_DIR = ROOT_DIR / "simulator"
if str(SIMULATOR_DIR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_DIR))

from core.event_scheduler.event_queue import EventQueue
from core.db import Database
from core.simulation_clock import get_simulation_now
from generators.purchase_order.purchase_order_created import generate_purchase_order
from master_simulator import run_simulation_cycle
from core.multi_domain import seed_multi_domain_events
from core.event_scheduler.event_registry import EVENT_REGISTRY


class SimulationManager:
    def __init__(self):
        self._runs = {}
        self._lock = threading.RLock()

    def start(self, simulation_time=None, duration_hours=None, run_window="morning"):
        simulation_id = f"SIM-{uuid.uuid4().hex[:8].upper()}"
        queue = EventQueue(memory_mode=False)
        record = {
            "simulation_id": simulation_id,
            "status": "RUNNING",
            "events_processed": 0,
            "queue": queue,
            "simulation_time": simulation_time or get_simulation_now(),
            "last_result": None,
            "error": None,
            "pause_requested": False,
            "initial_event_created": False,
            "seeded_events": [],
            "duration_hours": duration_hours,
            "run_window": run_window,
        }
        with self._lock:
            self._runs[simulation_id] = record
        threading.Thread(target=self._run, args=(simulation_id,), daemon=True).start()
        return self.status(simulation_id)

    def _run(self, simulation_id):
        with self._lock:
            record = self._runs[simulation_id]
        try:
            with self._lock:
                if record["pause_requested"]:
                    record["status"] = "PAUSED"
                    return
            with self._lock:
                record["status"] = "RUNNING"
            result = None
            for _ in range(100):
                with self._lock:
                    if record["pause_requested"]:
                        record["status"] = "PAUSED"
                        return
                    simulation_time = record["simulation_time"]
                    duration_hours = record.get("duration_hours")
                result = run_simulation_cycle(
                    simulation_time=simulation_time,
                    queue=record["queue"],
                    registry=EVENT_REGISTRY,
                    run_window=record.get("run_window", "morning"),
                )
                with Database() as db:
                    next_event = db.fetch_one(
                        """
                        SELECT COALESCE(
                            (payload->>'next_retry_time')::timestamptz,
                            (payload->>'event_scheduled_time')::timestamptz
                        ) AS scheduled_time
                        FROM event_outbox
                        WHERE status IN ('PENDING', 'RETRY_SCHEDULED')
                        ORDER BY COALESCE(
                            (payload->>'next_retry_time')::timestamptz,
                            (payload->>'event_scheduled_time')::timestamptz
                        ) ASC
                        LIMIT 1
                        """
                    )
                if not next_event:
                    break
                with self._lock:
                    next_scheduled_time = next_event["scheduled_time"]
                    if duration_hours is not None:
                        horizon = simulation_time + timedelta(hours=duration_hours)
                        next_scheduled_time = min(next_scheduled_time, horizon)
                    record["simulation_time"] = max(simulation_time, next_scheduled_time)
                    record["last_result"] = result
                    record["events_processed"] += len(result.get("processed_events", []))
                if duration_hours is not None and record["simulation_time"] >= simulation_time + timedelta(hours=duration_hours):
                    break
            with self._lock:
                record["last_result"] = result
                record["status"] = "COMPLETED"
        except Exception as exc:
            with self._lock:
                record["error"] = str(exc)
                record["status"] = "FAILED"

    def status(self, simulation_id):
        with self._lock:
            record = self._runs.get(simulation_id)
            if record is None:
                return None
            result = record.get("last_result") or {}
            return {
                "simulation_id": simulation_id,
                "events_processed": record["events_processed"],
                "queue_size": record["queue"].queue_size,
                "current_simulation_time": result.get("simulation_time", record["simulation_time"]).isoformat() if isinstance(result.get("simulation_time", record["simulation_time"]), datetime) else result.get("simulation_time", record["simulation_time"]),
                "run_window": record.get("run_window", "morning"),
                "status": record["status"],
                "error": record["error"],
            }

    def pause(self, simulation_id):
        with self._lock:
            record = self._runs.get(simulation_id)
            if record is None:
                return None
            record["pause_requested"] = True
            if record["status"] == "RUNNING":
                record["status"] = "PAUSED"
            return self.status(simulation_id)

    def resume(self, simulation_id):
        with self._lock:
            record = self._runs.get(simulation_id)
            if record is None:
                return None
            record["pause_requested"] = False
            if record["status"] == "PAUSED":
                record["status"] = "RUNNING"
                threading.Thread(target=self._run, args=(simulation_id,), daemon=True).start()
            return self.status(simulation_id)

    def report(self):
        with self._lock:
            return [self.status(simulation_id) for simulation_id in self._runs]


simulation_manager = SimulationManager()