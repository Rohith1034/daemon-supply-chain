from __future__ import annotations

import json
import os
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
from core.checkpointing import SimulationCheckpoint
from core.simulation_clock import get_simulation_now
from generators.purchase_order.purchase_order_created import generate_purchase_order
from master_simulator import run_simulation_cycle
from core.multi_domain import seed_multi_domain_events
from core.event_scheduler.event_registry import EVENT_REGISTRY
from api.services.run_lock import simulation_execution_lock

RUN_STATE_ROOT = ROOT_DIR / "output" / "simulation_runs" / "manager"


class _CorrelationScopedQueue:
    def __init__(self, queue, correlation_id):
        self._queue = queue
        self._correlation_id = str(correlation_id)

    def list_ready_events(self, *args, **kwargs):
        return [
            event for event in self._queue.list_ready_events(*args, **kwargs)
            if str(event.get("correlation_id")) == self._correlation_id
        ]

    @property
    def queue_size(self):
        return len(self.list_ready_events())

    def __getattr__(self, name):
        return getattr(self._queue, name)


class SimulationManager:
    def __init__(self):
        self._runs = {}
        self._lock = threading.RLock()
        self._restore_runs()

    def _restore_runs(self):
        if not RUN_STATE_ROOT.exists():
            return
        for report_path in RUN_STATE_ROOT.glob("*/report.json"):
            try:
                record = json.loads(report_path.read_text(encoding="utf-8"))
                simulation_id = record["simulation_id"]
                run_directory = RUN_STATE_ROOT / simulation_id
                checkpoint_path = run_directory / "checkpoint.json"
                checkpoint_time = SimulationCheckpoint(file_path=str(checkpoint_path)).read()
                saved_time = record.get("simulation_time")
                if isinstance(saved_time, str):
                    saved_time = datetime.fromisoformat(saved_time.replace("Z", "+00:00"))
                if saved_time is not None and saved_time.tzinfo is None:
                    saved_time = saved_time.replace(tzinfo=timezone.utc)
                simulation_time = checkpoint_time or saved_time or get_simulation_now()
                correlation_id = record.get("correlation_id")
                queue = EventQueue(memory_mode=False)
                if correlation_id:
                    queue = _CorrelationScopedQueue(queue, correlation_id)
                record.update({
                    "queue": queue,
                    "simulation_time": simulation_time,
                    "checkpoint_path": str(checkpoint_path),
                    "report_path": str(report_path),
                    "worker_active": False,
                })
                if record.get("status") == "RUNNING":
                    record["status"] = "PAUSED"
                    record["pause_requested"] = False
                    record["error"] = "Recovered after process restart; resume is required"
                    self._write_report(record)
                with self._lock:
                    self._runs[simulation_id] = record
            except (KeyError, OSError, ValueError, json.JSONDecodeError):
                continue

    def start(self, simulation_time=None, duration_hours=None, run_window="morning", correlation_id=None):
        simulation_id = f"SIM-{uuid.uuid4().hex[:8].upper()}"
        queue = EventQueue(memory_mode=False)
        if correlation_id is not None:
            queue = _CorrelationScopedQueue(queue, correlation_id)
        run_directory = RUN_STATE_ROOT / simulation_id
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
            "correlation_id": str(correlation_id) if correlation_id is not None else None,
            "checkpoint_path": str(run_directory / "checkpoint.json"),
            "report_path": str(run_directory / "report.json"),
            "worker_active": False,
        }
        with self._lock:
            self._runs[simulation_id] = record
            self._start_worker_locked(record)
        return self.status(simulation_id)

    def _start_worker_locked(self, record):
        if record.get("worker_active"):
            return
        record["worker_active"] = True
        record["status"] = "RUNNING"
        record["pause_requested"] = False
        self._write_report(record)
        threading.Thread(target=self._run, args=(record["simulation_id"],), daemon=True).start()

    def _run(self, simulation_id):
        try:
            with simulation_execution_lock():
                self._run_locked(simulation_id)
        except Exception as exc:
            with self._lock:
                record = self._runs.get(simulation_id)
                if record is not None:
                    record["error"] = str(exc)
                    record["status"] = "FAILED"
        finally:
            with self._lock:
                record = self._runs.get(simulation_id)
                if record is not None:
                    record["worker_active"] = False
                    self._write_report(record)

    def _run_locked(self, simulation_id):
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
                previous_simulation_now = os.environ.get("SIMULATION_NOW")
                os.environ["SIMULATION_NOW"] = simulation_time.isoformat()
                try:
                    result = run_simulation_cycle(
                        simulation_time=simulation_time,
                        queue=record["queue"],
                        checkpoint_path=record["checkpoint_path"],
                        registry=EVENT_REGISTRY,
                        run_window=record.get("run_window", "morning"),
                    )
                finally:
                    if previous_simulation_now is None:
                        os.environ.pop("SIMULATION_NOW", None)
                    else:
                        os.environ["SIMULATION_NOW"] = previous_simulation_now
                with Database() as db:
                    correlation_id = record.get("correlation_id")
                    next_event = db.fetch_one(
                        """
                        SELECT COALESCE(
                            (payload->>'next_retry_time')::timestamptz,
                            (payload->>'event_scheduled_time')::timestamptz
                        ) AS scheduled_time
                        FROM event_outbox
                        WHERE status IN ('PENDING', 'RETRY_SCHEDULED')
                                                    AND (%s IS NULL OR correlation_id=%s)
                        ORDER BY COALESCE(
                            (payload->>'next_retry_time')::timestamptz,
                            (payload->>'event_scheduled_time')::timestamptz
                        ) ASC
                        LIMIT 1
                        """,
                        (correlation_id, correlation_id),
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
        finally:
            with self._lock:
                self._write_report(record)

    @staticmethod
    def _write_report(record):
        report_path = Path(record["report_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            key: value
            for key, value in record.items()
            if key not in {"queue", "checkpoint_path", "report_path", "worker_active"}
        }
        temporary_path = report_path.with_suffix(".tmp")
        temporary_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        os.replace(temporary_path, report_path)

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
                "correlation_id": record.get("correlation_id"),
                "report_path": record.get("report_path"),
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
            self._write_report(record)
            return self.status(simulation_id)

    def resume(self, simulation_id):
        with self._lock:
            record = self._runs.get(simulation_id)
            if record is None:
                return None
            record["pause_requested"] = False
            if record["status"] == "PAUSED":
                if record.get("worker_active"):
                    record["status"] = "RUNNING"
                    self._write_report(record)
                else:
                    self._start_worker_locked(record)
            return self.status(simulation_id)

    def report(self):
        with self._lock:
            return [self.status(simulation_id) for simulation_id in self._runs]


simulation_manager = SimulationManager()