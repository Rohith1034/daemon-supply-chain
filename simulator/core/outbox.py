import uuid

from psycopg2.extras import Json



def publish_event(
        db,
        event_type,
        aggregate_type,
        aggregate_id,
        correlation_id,
        payload
):

    event_id = str(
        uuid.uuid4()
    )


    inserted = db.fetch_one(
        """
        INSERT INTO event_outbox
        (
            event_id,
            event_type,
            aggregate_type,
            aggregate_id,
            correlation_id,
            payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (
            aggregate_type,
            aggregate_id,
            event_type,
            correlation_id
        )
        WHERE status IN ('PENDING', 'RETRY_SCHEDULED', 'COMPLETED')
        DO NOTHING
        RETURNING event_id
        """,
        (
            event_id,
            event_type,
            aggregate_type,
            aggregate_id,
            str(correlation_id),
            Json(payload or {}),
        ),
    )

    if inserted:
        return str(inserted["event_id"])

    existing = db.fetch_one(
        """
        SELECT event_id
        FROM event_outbox
        WHERE aggregate_type=%s
          AND aggregate_id=%s
          AND event_type=%s
          AND correlation_id=%s
          AND status IN ('PENDING', 'RETRY_SCHEDULED', 'COMPLETED')
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (aggregate_type, aggregate_id, event_type, str(correlation_id)),
    )
    if existing:
        return str(existing["event_id"])

    raise RuntimeError(
        "Outbox insert was skipped but no matching event exists"
    )