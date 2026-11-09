import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dags"))

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_KAFKA_CONSUMER_INTEGRATION") != "1",
    reason="Requires the test PostgreSQL database and GCS bucket",
)


def test_gcs_and_postgres_event_replay_is_idempotent():
    import psycopg2
    from google.cloud import storage
    from simulator.core.config import DB_CONFIG

    from kafka_consumer.idempotency import IdempotencyService
    from kafka_consumer.message import validate_event

    event_id = f"integration-{uuid.uuid4()}"
    bucket_name = os.environ["GCS_BUCKET"]
    bucket = storage.Client().bucket(bucket_name)
    service = IdempotencyService.from_clients(
        lambda: psycopg2.connect(**DB_CONFIG, connect_timeout=10),
        bucket,
        30,
    )
    body = (
        '{"eventType":"PurchaseOrderCreated",'
        '"occurredAt":"2026-11-09T12:00:00+00:00",'
        '"purchaseOrder":{"poId":"PO-INTEGRATION"}}'
    ).encode()
    headers = [
        ("event_id", event_id.encode()),
        ("event_type", b"PurchaseOrderCreated"),
        ("aggregate_id", b"PO-INTEGRATION"),
        ("created_at", b"2026-11-09T12:00:00+00:00"),
    ]
    first = validate_event("purchase-order-events", 0, 1, body, headers)
    replay = validate_event("purchase-order-events", 0, 2, body, headers)

    first_result = service.process_valid(first, retry_count=0)
    replay_result = service.process_valid(replay, retry_count=0)
    assert first_result[0] == "SUCCESS"
    assert replay_result[0] == "DUPLICATE"
    assert first_result[1] == replay_result[1]

    connection = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM kafka_consumer_audit WHERE event_id = %s",
                (event_id,),
            )
            assert cursor.fetchone()[0] == 1
            cursor.execute(
                "DELETE FROM kafka_consumer_audit WHERE event_id = %s",
                (event_id,),
            )
        connection.commit()
    finally:
        connection.close()
        bucket.blob(first_result[1].removeprefix(f"gs://{bucket_name}/")).delete()
