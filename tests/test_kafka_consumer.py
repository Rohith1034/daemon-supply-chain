import json
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from kafka_consumer.config import ConsumerConfig
from kafka_consumer.consumer import KafkaConsumerService
from kafka_consumer.message import (
    MessageValidationError,
    validate_event,
)
from kafka_consumer.retry import retry_call
from kafka_consumer.storage import GcsObjectStore


def _event(offset=1, payload=None):
    payload = payload or {
        "eventType": "PurchaseOrderCreated",
        "occurredAt": "2026-11-09T12:00:00+00:00",
        "purchaseOrder": {"poId": "PO-1"},
    }
    return validate_event(
        "purchase-order-events",
        0,
        offset,
        json.dumps(payload).encode(),
        headers=[
            ("event_id", b"event-1"),
            ("event_type", b"PurchaseOrderCreated"),
            ("aggregate_id", b"PO-1"),
            ("created_at", b"2026-11-09T12:00:00+00:00"),
        ],
    )


def _config(**overrides):
    values = {
        "bootstrap_servers": "broker:9092",
        "group_id": "test-group",
        "client_id": "test-client",
        "security_protocol": "PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "",
        "sasl_password": "",
        "auto_offset_reset": "earliest",
        "max_retries": 0,
        "retry_backoff_seconds": 0.01,
        "retry_backoff_max_seconds": 0.1,
        "poll_timeout_seconds": 0.001,
        "idle_timeout_seconds": 0.001,
        "max_messages": 10,
        "gcs_bucket": "test-bucket",
        "gcs_timeout_seconds": 1,
        "topics": ("purchase-order-events",),
    }
    values.update(overrides)
    return ConsumerConfig(**values)


class FakeMessage:
    def __init__(self, value=b'{"payload":"ok"}', offset=1):
        self._value = value
        self._offset = offset

    def topic(self):
        return "purchase-order-events"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def error(self):
        return None

    def value(self):
        return self._value

    def headers(self):
        return [
            ("event_id", b"event-1"),
            ("event_type", b"PurchaseOrderCreated"),
            ("aggregate_id", b"PO-1"),
            ("created_at", b"2026-11-09T12:00:00+00:00"),
        ]

    def key(self):
        return b"PO-1"


class FakeConsumer:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.commits = []
        self.closed = False

    def subscribe(self, topics, on_revoke=None, on_lost=None):
        self.topics = topics
        self.on_revoke = on_revoke
        self.on_lost = on_lost

    def poll(self, _timeout):
        return next(self.messages, None)

    def commit(self, message, asynchronous):
        self.commits.append((message.offset(), asynchronous))

    def close(self):
        self.closed = True


def test_duplicate_event_payload_checksum_ignores_kafka_offset():
    first = _event(offset=10)
    replay = _event(offset=99)

    assert first.event_id == replay.event_id
    assert first.payload_sha256 == replay.payload_sha256
    assert first.bronze_bytes() == replay.bronze_bytes()


def test_bronze_object_path_uses_stable_date_hour_and_event_id():
    assert GcsObjectStore.raw_object_name(_event()) == (
        "raw/purchase-order-events/PurchaseOrderCreated/2026/11/09/"
        "hour=12/ingestion_time=20261109T120000/event_event-1.json"
    )


@pytest.mark.parametrize(
    "topic, value, headers, expected",
    [
        (
            "purchase-order-events",
            b'{"eventType":"PurchaseOrderCreated"}',
            [],
            "event_id",
        ),
        (
            "purchase-order-events",
            b'{"eventType":"DefinitelyUnknown"}',
            [
                ("event_id", b"e1"),
                ("created_at", b"2026-01-01T00:00:00Z"),
                ("aggregate_id", b"a1"),
            ],
            "Unknown event type",
        ),
        ("purchase-order-events", b"{bad json", [], "Invalid JSON"),
        (
            "purchase-order-events",
            b'{"eventType":"PurchaseOrderCreated","occurredAt":"not-a-time"}',
            [
                ("event_id", b"e1"),
                ("aggregate_id", b"a1"),
            ],
            "ISO-8601",
        ),
    ],
)
def test_invalid_messages_raise_validation_error(topic, value, headers, expected):
    with pytest.raises(MessageValidationError, match=expected):
        validate_event(topic, 0, 1, value, headers=headers)


def test_empty_json_object_is_missing_required_payload():
    with pytest.raises(MessageValidationError, match="payload"):
        validate_event(
            "purchase-order-events",
            0,
            1,
            b"{}",
            headers=[
                ("event_id", b"event-1"),
                ("event_type", b"PurchaseOrderCreated"),
                ("aggregate_id", b"PO-1"),
                ("created_at", b"2026-11-09T12:00:00+00:00"),
            ],
        )


def test_invalid_utf8_header_is_quarantinable_validation_error():
    with pytest.raises(MessageValidationError, match="UTF-8"):
        validate_event(
            "purchase-order-events",
            0,
            1,
            b'{"payload":{"purchaseOrder":"PO-1"}}',
            headers=[("event_id", b"\xff")],
        )


def test_retry_call_uses_bounded_exponential_backoff():
    attempts = []
    delays = []

    def operation():
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("temporary")
        return "done"

    result = retry_call(
        operation,
        max_attempts=3,
        initial_backoff=1,
        max_backoff=10,
        sleep=delays.append,
        random_value=lambda: 0,
    )

    assert result == "done"
    assert delays == [0.5, 1.0]


def test_consumer_commits_after_processing_and_closes():
    message = FakeMessage()
    consumer = FakeConsumer([message])
    processed = []

    class Processor:
        def process_valid(self, event, retry_count):
            processed.append((event.event_id, retry_count))
            return "SUCCESS", "gs://test-bucket/raw/e.json"

        def process_invalid(self, event, retry_count):
            raise AssertionError("Message should validate")

    service = KafkaConsumerService(
        _config(max_messages=1),
        lambda _config: consumer,
        Processor(),
        threading.Event(),
    )

    assert service.run() == {"status": "success", "processed_messages": 1}
    assert processed == [("event-1", 0)]
    assert consumer.commits == [(1, False)]
    assert consumer.closed


def test_consumer_does_not_commit_when_processing_fails():
    consumer = FakeConsumer([FakeMessage()])

    class Processor:
        def process_valid(self, _event, _retry_count):
            raise RuntimeError("database unavailable")

        def process_invalid(self, _event, _retry_count):
            raise AssertionError("Message should validate")

    service = KafkaConsumerService(
        _config(max_messages=1),
        lambda _config: consumer,
        Processor(),
        threading.Event(),
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        service.run()
    assert consumer.commits == []
    assert consumer.closed


@pytest.mark.parametrize("value", [None, b""])
def test_tombstone_and_empty_message_are_committed_without_processing(value):
    consumer = FakeConsumer([FakeMessage(value=value)])

    class Processor:
        def process_valid(self, *_):
            raise AssertionError("No payload should be processed")

        def process_invalid(self, *_):
            raise AssertionError("No payload should be quarantined")

    service = KafkaConsumerService(
        _config(max_messages=1),
        lambda _config: consumer,
        Processor(),
        threading.Event(),
    )

    assert service.run()["processed_messages"] == 1
    assert consumer.commits == [(1, False)]


def test_invalid_message_is_committed_only_after_quarantine():
    events = []
    message = FakeMessage(value=b"{invalid")

    class RecordingConsumer(FakeConsumer):
        def commit(self, message, asynchronous):
            events.append("commit")
            super().commit(message, asynchronous)

    class Processor:
        def process_valid(self, *_):
            raise AssertionError("Invalid message should not be processed")

        def process_invalid(self, _event, _retry_count):
            events.append("quarantine")
            return "QUARANTINED", "gs://test-bucket/quarantine/event.json"

    consumer = RecordingConsumer([message])
    service = KafkaConsumerService(
        _config(max_messages=1),
        lambda _config: consumer,
        Processor(),
        threading.Event(),
    )

    assert service.run()["processed_messages"] == 1
    assert events == ["quarantine", "commit"]
