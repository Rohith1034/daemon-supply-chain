import json
import os
import uuid
from datetime import datetime, timezone

from psycopg2.extras import Json

from core.db import Database
from core.simulation_clock import get_simulation_now


PRIORITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


class EventQueue:
    """Persist and query future events in the outbox-backed event stream."""

    def __init__(self, memory_mode: bool = False):
        self.memory_mode = memory_mode or os.getenv("SIMULATOR_MEMORY_EVENT_QUEUE", "0") == "1"
        self._memory_events = []
        self.execution_history = []
        self._current_ready_time = None
        self._state_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "event_queue_state.json")
        if os.getenv("SIMULATOR_PERSIST_MEMORY_QUEUE", "0") == "1":
            self._load_state()

    @property
    def queue_size(self):
        if not self.memory_mode:
            reference_time = self._current_ready_time or get_simulation_now()
            with Database() as db:
                row = db.fetch_one(
                    """
                    SELECT COUNT(*) AS queue_size
                    FROM event_outbox
                    WHERE status IN (%s, %s)
                      AND COALESCE(
                          (payload ->> 'next_retry_time')::timestamptz,
                          (payload ->> 'event_scheduled_time')::timestamptz,
                          created_at
                      ) <= %s
                    """,
                    ("PENDING", "RETRY_SCHEDULED", reference_time),
                )
                return int(row["queue_size"] if row else 0)
        reference_time = self._current_ready_time or get_simulation_now()
        return sum(
            1
            for row in self._memory_events
            if row.get("status") in {"PENDING", "RETRY_SCHEDULED"}
            and self._normalize_datetime(row.get("scheduled_time") or row.get("event_scheduled_time")) <= reference_time
        )

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
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _priority_rank(priority):
        return PRIORITY_ORDER.get(str(priority or "MEDIUM").upper(), PRIORITY_ORDER["MEDIUM"])

    @staticmethod
    def _event_history_entry(row, status, *, error_message=None, processed_at=None):
        history = dict(row)
        history["event_name"] = history.get("event_name") or history.get("event_type")
        history["status"] = status
        history["processed_at"] = (processed_at or get_simulation_now()).isoformat()
        if error_message is not None:
            history["last_error"] = str(error_message)
        return history

    @staticmethod
    def _json_safe(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, dict):
            return {str(k): EventQueue._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [EventQueue._json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [EventQueue._json_safe(item) for item in value]
        return value

    def _save_state(self):
        if not self.memory_mode:
            return
        payload = {"events": [], "history": []}
        for row in self._memory_events:
            payload["events"].append(self._json_safe(row))
        for entry in self.execution_history:
            payload["history"].append(self._json_safe(entry))
        os.makedirs(os.path.dirname(self._state_path), exist_ok=True)
        with open(self._state_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def _load_state(self):
        if not self.memory_mode or not os.path.exists(self._state_path):
            return
        try:
            with open(self._state_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle) or {"events": [], "history": []}
        except (json.JSONDecodeError, OSError):
            return
        for row in payload.get("events", []):
            if "created_at" in row and isinstance(row["created_at"], str):
                row["created_at"] = self._normalize_datetime(row["created_at"])
            self._memory_events.append(row)
        for entry in payload.get("history", []):
            if "processed_at" in entry and isinstance(entry["processed_at"], str):
                entry["processed_at"] = self._normalize_datetime(entry["processed_at"])
            self.execution_history.append(entry)

    def _ensure_memory_row(
        self,
        event_id,
        event_type,
        aggregate_type=None,
        aggregate_id=None,
        correlation_id=None,
        payload=None,
        scheduled_time=None,
        status="PENDING",
        priority="MEDIUM",
        retry_count=0,
        max_retry_count=3,
        next_retry_time=None,
    ):
        for row in self._memory_events:
            if row["event_id"] == event_id:
                return row
        row = {
            "event_id": str(event_id),
            "event_name": str(event_type),
            "event_type": str(event_type),
            "aggregate_type": aggregate_type or "scheduled_events",
            "aggregate_id": str(aggregate_id) if aggregate_id is not None else "SIM-ROOT",
            "correlation_id": str(correlation_id) if correlation_id is not None else str(uuid.uuid4()),
            "payload": dict(payload or {}),
            "status": status,
            "scheduled_time": self._normalize_datetime(scheduled_time or get_simulation_now()).isoformat(),
            "event_created_time": get_simulation_now().isoformat(),
            "event_started_time": None,
            "event_completed_time": None,
            "event_scheduled_time": self._normalize_datetime(scheduled_time or get_simulation_now()).isoformat(),
            "created_at": get_simulation_now(),
            "priority": str(priority).upper(),
            "retry_count": int(retry_count),
            "max_retry_count": int(max_retry_count),
            "next_retry_time": None if next_retry_time is None else self._normalize_datetime(next_retry_time).isoformat(),
            "last_error": None,
        }
        self._memory_events.append(row)
        return row

    def _should_block_causal_insert(self, event_type, aggregate_id, payload):
        if self.memory_mode:
            return False
        if event_type != "SupplierShipmentCreated":
            return False
        po_id = (payload or {}).get("po_id") or aggregate_id
        if po_id in {None, "", "SIM-ROOT"}:
            return False
        with Database() as db:
            row = db.fetch_one(
                """
                SELECT po_status
                FROM purchase_orders
                WHERE po_id = %s
                LIMIT 1
                """,
                (str(po_id),),
            )
            return not (row and row.get("po_status") == "APPROVED")

    def insert_future_event(
        self,
        event_type,
        aggregate_type,
        aggregate_id,
        correlation_id,
        payload,
        scheduled_time=None,
        status="PENDING",
        event_id=None,
        priority="MEDIUM",
        retry_count=0,
        max_retry_count=3,
        next_retry_time=None,
    ):
        if self._should_block_causal_insert(event_type, aggregate_id, payload):
            return None

        resolved_event_id = str(event_id or uuid.uuid4())
        now = get_simulation_now()
        scheduled_dt = self._normalize_datetime(scheduled_time or now)
        business_payload = dict(payload or {})
        business_statuses = {"PENDING", "RETRY_SCHEDULED", "COMPLETED"}

        if self.memory_mode:
            for row in self._memory_events:
                if (
                    row.get("correlation_id") == str(correlation_id)
                    and row.get("event_type") == event_type
                    and row.get("status") in business_statuses
                ):
                    return row["event_id"]
                if (
                    row.get("aggregate_id") == aggregate_id
                    and row.get("event_type") == event_type
                    and row.get("status") in business_statuses
                ):
                    return row["event_id"]
            row = {
                "event_id": resolved_event_id,
                "event_name": event_type,
                "event_type": event_type,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "correlation_id": str(correlation_id),
                "payload": business_payload,
                "status": status,
                "scheduled_time": scheduled_dt.isoformat(),
                "event_created_time": now.isoformat(),
                "event_started_time": None,
                "event_completed_time": None,
                "event_scheduled_time": scheduled_dt.isoformat(),
                "created_at": now,
                "priority": str(priority).upper(),
                "retry_count": int(retry_count),
                "max_retry_count": int(max_retry_count),
                "next_retry_time": None if next_retry_time is None else self._normalize_datetime(next_retry_time).isoformat(),
                "last_error": None,
            }
            self._memory_events.append(row)
            self._save_state()
            return resolved_event_id

        with Database() as db:
            payload_blob = dict(payload or {})
            payload_blob.setdefault("event_created_time", now.isoformat())
            payload_blob["event_scheduled_time"] = scheduled_dt.isoformat()
            payload_blob["retry_count"] = int(retry_count)
            payload_blob["max_retry_count"] = int(max_retry_count)
            payload_blob["priority"] = str(priority).upper()
            payload_blob["next_retry_time"] = None if next_retry_time is None else self._normalize_datetime(next_retry_time).isoformat()
            payload_blob["last_error"] = None
            row = db.fetch_one(
                """
                SELECT event_id
                FROM event_outbox
                WHERE correlation_id = %s
                  AND event_type = %s
                  AND status IN (%s, %s, %s)
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (str(correlation_id), event_type, "PENDING", "RETRY_SCHEDULED", "COMPLETED"),
            )
            if row:
                return str(row["event_id"])
            row = db.fetch_one(
                """
                SELECT event_id
                FROM event_outbox
                WHERE aggregate_id = %s
                  AND event_type = %s
                  AND status IN (%s, %s, %s)
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (aggregate_id, event_type, "PENDING", "RETRY_SCHEDULED", "COMPLETED"),
            )
            if row:
                return str(row["event_id"])
            db.execute(
                """
                INSERT INTO event_outbox (
                    event_id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    correlation_id,
                    payload,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    resolved_event_id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    correlation_id,
                    Json(payload_blob),
                    status,
                ),
            )
        return resolved_event_id

    def list_ready_events(self, simulation_time=None, from_time=None):
        target_time = self._normalize_datetime(simulation_time or get_simulation_now())
        from_dt = self._normalize_datetime(from_time) if from_time is not None else None
        self._current_ready_time = target_time

        if self.memory_mode:
            ready = []
            for row in self._memory_events:
                current_status = row.get("status")
                if current_status not in {"PENDING", "RETRY_SCHEDULED"}:
                    continue
                scheduled_at = self._normalize_datetime(row.get("scheduled_time") or row.get("event_scheduled_time"))
                if from_dt is not None and scheduled_at < from_dt:
                    continue
                if scheduled_at <= target_time:
                    if current_status == "RETRY_SCHEDULED":
                        retry_time = row.get("next_retry_time")
                        if retry_time is not None and self._normalize_datetime(retry_time) > target_time:
                            continue
                    ready.append(row)
            ready.sort(key=lambda row: (self._priority_rank(row.get("priority") or row.get("payload", {}).get("priority")), self._normalize_datetime(row.get("scheduled_time") or row.get("event_scheduled_time"))))
            return ready

        with Database() as db:
            clauses = [
                "status IN (%s, %s)",
                "payload ->> 'event_scheduled_time' IS NOT NULL",
                "COALESCE((payload ->> 'next_retry_time')::timestamptz, (payload ->> 'event_scheduled_time')::timestamptz) <= %s",
            ]
            params = ["PENDING", "RETRY_SCHEDULED", target_time.isoformat()]
            if from_dt is not None:
                clauses.append("COALESCE((payload ->> 'event_scheduled_time')::timestamptz, created_at) >= %s")
                params.append(from_dt.isoformat())
            rows = db.fetch_all(
                f"""
                SELECT *
                FROM event_outbox
                WHERE {' AND '.join(clauses)}
                ORDER BY COALESCE(
                    (payload ->> 'next_retry_time')::timestamptz,
                    (payload ->> 'event_scheduled_time')::timestamptz
                ) ASC
                """,
                tuple(params),
            )
            normalized = []
            for row in rows:
                payload = dict(row["payload"]) if row.get("payload") else {}
                if row["status"] == "RETRY_SCHEDULED":
                    retry_time = payload.get("next_retry_time")
                    if retry_time is not None and self._normalize_datetime(retry_time) > target_time:
                        continue
                normalized_row = dict(row)
                normalized_row["event_id"] = str(normalized_row["event_id"])
                normalized_row["payload"] = payload
                normalized.append(normalized_row)
            normalized.sort(key=lambda row: (self._priority_rank(row.get("priority") or row["payload"].get("priority")), self._normalize_datetime(row["payload"].get("event_scheduled_time"))))
            return normalized

    def update_event_aggregate_id(self, event_id, aggregate_id, payload=None):
        if aggregate_id in {None, "", "SIM-ROOT"}:
            return
        if self.memory_mode:
            for row in self._memory_events:
                if row.get("event_id") == str(event_id):
                    row["aggregate_id"] = str(aggregate_id)
                    row["payload"] = dict(row.get("payload") or {})
                    row["payload"]["aggregate_id"] = str(aggregate_id)
                    return
            return

        with Database() as db:
            row = db.fetch_one(
                "SELECT event_type, aggregate_id FROM event_outbox WHERE event_id = %s",
                (str(event_id),),
            )
            if not row:
                return
            existing = db.fetch_one(
                "SELECT event_id FROM event_outbox WHERE event_type = %s AND aggregate_id = %s AND event_id != %s LIMIT 1",
                (row["event_type"], str(aggregate_id), str(event_id)),
            )
            if existing:
                return
            payload_blob = dict(payload or {})
            payload_blob["aggregate_id"] = str(aggregate_id)
            db.execute(
                """
                UPDATE event_outbox
                SET aggregate_id = %s,
                    payload = %s
                WHERE event_id = %s
                """,
                (str(aggregate_id), Json(payload_blob), str(event_id)),
            )

    def mark_event_status(self, event_id, status, processed_at=None, error_message=None, retry_count=None, next_retry_time=None, priority=None):
        if self.memory_mode:
            row = self._ensure_memory_row(
                event_id,
                event_type="UNKNOWN",
                aggregate_type="scheduled_events",
                aggregate_id="SIM-ROOT",
                correlation_id=None,
                payload={},
                scheduled_time=get_simulation_now(),
                status=status,
                priority=priority or "MEDIUM",
                retry_count=retry_count or 0,
                next_retry_time=next_retry_time,
            )
            row["status"] = status
            row["priority"] = str(priority or row.get("priority") or row.get("payload", {}).get("priority") or "MEDIUM").upper()
            row["payload"] = dict(row.get("payload") or {})
            if processed_at:
                row["event_completed_time"] = self._normalize_datetime(processed_at).isoformat()
            if error_message is not None:
                row["last_error"] = str(error_message)
                row["payload"]["last_error"] = str(error_message)
            if retry_count is not None:
                row["retry_count"] = int(retry_count)
                row["payload"]["retry_count"] = int(retry_count)
            if next_retry_time is not None:
                next_retry_dt = self._normalize_datetime(next_retry_time)
                row["next_retry_time"] = next_retry_dt.isoformat()
                row["payload"]["next_retry_time"] = next_retry_dt.isoformat()
                row["scheduled_time"] = next_retry_dt.isoformat()
                row["event_scheduled_time"] = next_retry_dt.isoformat()
                row["payload"]["event_scheduled_time"] = next_retry_dt.isoformat()
            else:
                row["next_retry_time"] = None
                row["payload"]["next_retry_time"] = None
            if status in {"COMPLETED", "FAILED", "SKIPPED"}:
                history_entry = self._event_history_entry(row, status, error_message=error_message, processed_at=processed_at)
                self.execution_history.append(history_entry)
                self._save_state()
                return history_entry
            self._save_state()
            return row

        with Database() as db:
            row = db.fetch_one(
                """
                SELECT payload
                FROM event_outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )
            payload = dict(row["payload"]) if row and row.get("payload") else {}
            if error_message is not None:
                payload["last_error"] = str(error_message)
            if retry_count is not None:
                payload["retry_count"] = int(retry_count)
            if priority is not None:
                payload["priority"] = str(priority).upper()
            if next_retry_time is not None:
                next_retry_dt = self._normalize_datetime(next_retry_time)
                payload["next_retry_time"] = next_retry_dt.isoformat()
                payload["event_scheduled_time"] = next_retry_dt.isoformat()
            elif "next_retry_time" in payload and status == "RETRY_SCHEDULED":
                payload["next_retry_time"] = None
            if status == "RETRY_SCHEDULED" and next_retry_time is not None:
                payload["status"] = status
            db.execute(
                """
                UPDATE event_outbox
                SET status = %s,
                    payload = %s,
                    published_at = COALESCE(published_at, %s)
                WHERE event_id = %s
                """,
                (status, Json(payload), processed_at or get_simulation_now(), event_id),
            )
            return None
