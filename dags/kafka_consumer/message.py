import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from kafka_consumer.metadata import REQUIRED_FIELDS, event_types_for_topic


class MessageValidationError(ValueError):
    pass


@dataclass(frozen=True)
class SourcePosition:
    topic: str
    partition: int
    offset: int

    @property
    def key(self):
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass(frozen=True)
class ValidatedEvent:
    event_id: str
    event_type: str
    created_at: str
    aggregate_id: str
    payload: object
    topic: str
    partition: int
    offset: int
    raw_value: bytes
    payload_sha256: str

    @property
    def source(self):
        return SourcePosition(self.topic, self.partition, self.offset)

    def bronze_envelope(self):
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "created_at": self.created_at,
            "aggregate_id": self.aggregate_id,
            "payload": self.payload,
        }

    def bronze_bytes(self):
        return json.dumps(
            self.bronze_envelope(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")

    @property
    def bronze_sha256(self):
        return hashlib.sha256(self.bronze_bytes()).hexdigest()


@dataclass(frozen=True)
class InvalidEvent:
    source: SourcePosition
    raw_value: bytes
    reason: str
    event_id: str | None = None

    @property
    def dedupe_key(self):
        return f"quarantine:source:{self.source.key}"

    def quarantine_bytes(self):
        import base64

        return json.dumps(
            {
                "event_id": self.event_id,
                "error": self.reason,
                "kafka": {
                    "topic": self.source.topic,
                    "partition": self.source.partition,
                    "offset": self.source.offset,
                },
                "raw_value_base64": base64.b64encode(self.raw_value).decode("ascii"),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")


def decode_headers(headers):
    decoded = {}
    for name, value in headers or ():
        if value is None:
            continue
        try:
            decoded[name] = (
                value.decode("utf-8")
                if isinstance(value, bytes)
                else str(value)
            )
        except UnicodeDecodeError as exc:
            raise MessageValidationError(
                f"Kafka header {name!r} is not valid UTF-8"
            ) from exc
    return decoded


def validate_event(topic, partition, offset, raw_value, headers=None, key=None):
    source = SourcePosition(topic, partition, offset)
    if raw_value is None:
        raise MessageValidationError("Null Kafka payload (tombstone)")

    headers = decode_headers(headers)
    try:
        document = json.loads(raw_value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MessageValidationError(f"Invalid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise MessageValidationError("Kafka JSON value must be an object")

    event_id = str(document.get("event_id") or headers.get("event_id") or "").strip()
    event_type = str(
        document.get("event_type")
        or document.get("eventType")
        or headers.get("event_type")
        or ""
    ).strip()
    created_at = str(
        document.get("created_at")
        or document.get("createdAt")
        or document.get("occurredAt")
        or headers.get("created_at")
        or ""
    ).strip()
    aggregate_id = str(
        document.get("aggregate_id")
        or document.get("aggregateId")
        or headers.get("aggregate_id")
        or key
        or ""
    ).strip()
    payload = document["payload"] if "payload" in document else document
    if "payload" not in document and not document:
        raise MessageValidationError("Missing required field: payload")

    normalized_fields = {
        "event_id": event_id,
        "event_type": event_type,
        "created_at": created_at,
        "aggregate_id": aggregate_id,
        "payload": payload,
    }
    for name in REQUIRED_FIELDS:
        if normalized_fields.get(name) is None or normalized_fields.get(name) == "":
            raise MessageValidationError(f"Missing required field: {name}")
    try:
        datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MessageValidationError(
            "created_at must be a valid ISO-8601 timestamp"
        ) from exc

    try:
        known_event_types = event_types_for_topic(topic)
    except ValueError as exc:
        raise MessageValidationError(str(exc)) from exc
    if event_type not in known_event_types:
        raise MessageValidationError(
            f"Unknown event type {event_type!r} for topic {topic!r}"
        )

    try:
        canonical_payload = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise MessageValidationError(f"Payload is not valid JSON: {exc}") from exc

    return ValidatedEvent(
        event_id=event_id,
        event_type=event_type,
        created_at=created_at,
        aggregate_id=aggregate_id,
        payload=payload,
        topic=topic,
        partition=partition,
        offset=offset,
        raw_value=raw_value,
        payload_sha256=hashlib.sha256(canonical_payload).hexdigest(),
    )
