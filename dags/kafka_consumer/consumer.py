import signal
import threading
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError

from kafka_consumer.config import ConsumerConfig
from kafka_consumer.idempotency import IdempotencyService
from kafka_consumer.logging_utils import log_event
from kafka_consumer.message import (
    InvalidEvent,
    MessageValidationError,
    SourcePosition,
    validate_event,
)
from kafka_consumer.retry import retry_call


@dataclass
class KafkaConsumerService:
    config: ConsumerConfig
    consumer_factory: object
    processor: object
    stop_event: threading.Event

    def _log_retry(self, message, attempt, delay, error):
        log_event(
            "warning",
            "Retrying message processing",
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            retry_count=attempt,
            retry_delay_seconds=delay,
            error_message=str(error),
        )

    def run(self):
        kafka_consumer = self.consumer_factory(
            self.config.kafka_consumer_config()
        )
        processed = 0
        last_message_at = time.monotonic()

        def on_revoke(_consumer, partitions):
            log_event(
                "info",
                "Partitions revoked; only synchronously committed offsets are retained",
                partitions=[
                    {"topic": item.topic, "partition": item.partition}
                    for item in partitions
                ],
            )

        def on_lost(_consumer, partitions):
            log_event(
                "warning",
                "Partitions lost; uncommitted work will be replayed idempotently",
                partitions=[
                    {"topic": item.topic, "partition": item.partition}
                    for item in partitions
                ],
            )

        try:
            kafka_consumer.subscribe(
                list(self.config.topics),
                on_revoke=on_revoke,
                on_lost=on_lost,
            )
            while not self.stop_event.is_set() and processed < self.config.max_messages:
                message = kafka_consumer.poll(self.config.poll_timeout_seconds)
                if message is None:
                    if (
                        self.config.idle_timeout_seconds
                        and time.monotonic() - last_message_at
                        >= self.config.idle_timeout_seconds
                    ):
                        break
                    continue
                last_message_at = time.monotonic()
                if message.error():
                    error = message.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    if error.retriable():
                        log_event(
                            "warning",
                            "Kafka poll error is retriable; waiting for recovery",
                            error_message=str(error),
                        )
                        continue
                    raise RuntimeError(f"Kafka poll failed: {error}")

                started = time.monotonic()
                raw_value = message.value()
                source = SourcePosition(
                    message.topic(), message.partition(), message.offset()
                )

                if raw_value is None or raw_value == b"":
                    log_event(
                        "warning" if raw_value is None else "info",
                        "Skipping Kafka tombstone" if raw_value is None else "Skipping empty Kafka message",
                        topic=source.topic,
                        partition=source.partition,
                        offset=source.offset,
                    )
                    kafka_consumer.commit(message=message, asynchronous=False)
                    processed += 1
                    continue

                try:
                    event = validate_event(
                        source.topic,
                        source.partition,
                        source.offset,
                        raw_value,
                        headers=message.headers(),
                        key=message.key(),
                    )
                except MessageValidationError as exc:
                    from kafka_consumer.message import decode_headers

                    try:
                        headers = decode_headers(message.headers())
                    except MessageValidationError:
                        headers = {}
                    try:
                        partial_document = __import__("json").loads(raw_value)
                    except (UnicodeDecodeError, ValueError):
                        partial_document = {}
                    event_id = (
                        partial_document.get("event_id")
                        or headers.get("event_id")
                    ) if isinstance(partial_document, dict) else headers.get("event_id")
                    invalid = InvalidEvent(
                        source=source,
                        raw_value=raw_value,
                        reason=str(exc),
                        event_id=event_id,
                    )
                    quarantine_attempts = 0

                    def quarantine():
                        nonlocal quarantine_attempts
                        quarantine_attempts += 1
                        return self.processor.process_invalid(
                            invalid,
                            quarantine_attempts - 1,
                        )

                    status, gcs_path = retry_call(
                        quarantine,
                        max_attempts=self.config.max_retries + 1,
                        initial_backoff=self.config.retry_backoff_seconds,
                        max_backoff=self.config.retry_backoff_max_seconds,
                        on_retry=lambda attempt, delay, error: self._log_retry(
                            message, attempt, delay, error
                        ),
                    )
                    kafka_consumer.commit(message=message, asynchronous=False)
                    processed += 1
                    log_event(
                        "error",
                        "Invalid message quarantined",
                        event_id=invalid.event_id,
                        topic=source.topic,
                        partition=source.partition,
                        offset=source.offset,
                        processing_time_seconds=time.monotonic() - started,
                        retry_count=quarantine_attempts - 1,
                        gcs_path=gcs_path,
                        status=status,
                        error_message=invalid.reason,
                    )
                    continue

                attempts = 0

                def process():
                    nonlocal attempts
                    attempts += 1
                    return self.processor.process_valid(event, attempts - 1)

                status, gcs_path = retry_call(
                    process,
                    max_attempts=self.config.max_retries + 1,
                    initial_backoff=self.config.retry_backoff_seconds,
                    max_backoff=self.config.retry_backoff_max_seconds,
                    on_retry=lambda attempt, delay, error: self._log_retry(
                        message, attempt, delay, error
                    ),
                )
                kafka_consumer.commit(message=message, asynchronous=False)
                processed += 1
                is_quarantined = status == "QUARANTINED"
                log_event(
                    "critical" if is_quarantined else "info",
                    (
                        "Conflicting event_id quarantined"
                        if is_quarantined
                        else "Kafka message processed"
                    ),
                    event_id=event.event_id,
                    topic=source.topic,
                    partition=source.partition,
                    offset=source.offset,
                    processing_time_seconds=time.monotonic() - started,
                    retry_count=attempts - 1,
                    gcs_path=gcs_path,
                    status=status,
                    error_message=(
                        "event_id was previously processed with a different payload"
                        if is_quarantined
                        else None
                    ),
                )
            return {"status": "success", "processed_messages": processed}
        finally:
            kafka_consumer.close()


def _connection_factory():
    import psycopg2

    from simulator.core.config import DB_CONFIG

    return psycopg2.connect(**DB_CONFIG, connect_timeout=10)


def main_consumer():
    config = ConsumerConfig.from_env()
    from google.cloud import storage

    bucket = storage.Client().bucket(config.gcs_bucket)
    processor = IdempotencyService.from_clients(
        _connection_factory,
        bucket,
        config.gcs_timeout_seconds,
    )
    stop_event = threading.Event()
    previous_handlers = {}

    def request_shutdown(signum, _frame):
        log_event("info", "Shutdown requested", signal=signum)
        stop_event.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, request_shutdown)
    try:
        service = KafkaConsumerService(
            config=config,
            consumer_factory=Consumer,
            processor=processor,
            stop_event=stop_event,
        )
        return service.run()
    finally:
        for signum, previous_handler in previous_handlers.items():
            signal.signal(signum, previous_handler)
