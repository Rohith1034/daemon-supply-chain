from datetime import datetime, timezone

from kafka_consumer.storage import GcsObjectConflictError


class AuditRepository:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def process_success(self, event, raw_store, retry_count):
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (event.event_id,),
                )
                cursor.execute(
                    """
                    SELECT payload_sha256, status, gcs_path
                    FROM kafka_consumer_audit
                    WHERE event_id = %s AND status = 'SUCCESS'
                    FOR UPDATE
                    """,
                    (event.event_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    old_checksum = existing[0]
                    if old_checksum == event.payload_sha256:
                        connection.commit()
                        return "DUPLICATE", existing[2]

                    return self._quarantine_conflict(
                        cursor,
                        connection,
                        event,
                        raw_store,
                        retry_count,
                        "event_id was previously processed with a different payload",
                    )

                try:
                    object_path = raw_store.write_event(event)
                except GcsObjectConflictError as exc:
                    return self._quarantine_conflict(
                        cursor,
                        connection,
                        event,
                        raw_store,
                        retry_count,
                        str(exc),
                    )

                self._insert_audit(
                    cursor,
                    dedupe_key=f"event:{event.event_id}",
                    event=event,
                    status="SUCCESS",
                    gcs_path=object_path,
                    checksum=event.bronze_sha256,
                    error_message=None,
                    retry_count=retry_count,
                )
            connection.commit()
            return "SUCCESS", object_path
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _quarantine_conflict(
        self,
        cursor,
        connection,
        event,
        raw_store,
        retry_count,
        reason,
    ):
        path = raw_store.quarantine_conflict(event)
        self._insert_audit(
            cursor,
            dedupe_key=f"conflict:{event.event_id}:{event.payload_sha256}",
            event=event,
            status="QUARANTINED",
            gcs_path=path,
            checksum=event.bronze_sha256,
            error_message=reason,
            retry_count=retry_count,
        )
        connection.commit()
        return "QUARANTINED", path

    def record_quarantine(self, invalid_event, raw_store, retry_count):
        data = invalid_event.quarantine_bytes()
        digest = raw_store.sha256(data)
        path = raw_store.write_quarantine(
            invalid_event.source.key,
            data,
            digest,
        )
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                self._insert_audit(
                    cursor,
                    dedupe_key=invalid_event.dedupe_key,
                    event=None,
                    source=invalid_event.source,
                    event_id=invalid_event.event_id,
                    status="QUARANTINED",
                    gcs_path=path,
                    checksum=digest,
                    payload_sha256=None,
                    error_message=invalid_event.reason,
                    retry_count=retry_count,
                )
                connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return path

    @staticmethod
    def _insert_audit(
        cursor,
        *,
        dedupe_key,
        event=None,
        source=None,
        event_id=None,
        status,
        gcs_path,
        checksum,
        error_message,
        retry_count,
        payload_sha256=None,
    ):
        if event is not None:
            source = event.source
            event_id = event.event_id
            payload_sha256 = event.payload_sha256
        cursor.execute(
            """
            INSERT INTO kafka_consumer_audit (
                dedupe_key, event_id, kafka_topic, kafka_partition,
                kafka_offset, status, processed_at, gcs_path, checksum,
                payload_sha256, retry_count, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dedupe_key) DO NOTHING
            """,
            (
                dedupe_key,
                event_id,
                source.topic,
                source.partition,
                source.offset,
                status,
                datetime.now(timezone.utc),
                gcs_path,
                checksum,
                payload_sha256,
                retry_count,
                error_message,
            ),
        )
        cursor.execute(
            """
            SELECT status, checksum, gcs_path
            FROM kafka_consumer_audit
            WHERE dedupe_key = %s
            """,
            (dedupe_key,),
        )
        inserted = cursor.fetchone()
        if inserted is None or inserted[1] != checksum:
            raise RuntimeError(
                f"Audit key {dedupe_key!r} already exists with different content"
            )
