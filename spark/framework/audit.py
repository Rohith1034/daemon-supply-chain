import json
import os
from datetime import datetime, timezone


def _connection_factory():
    import psycopg2

    database_url = os.getenv("SIMULATOR_DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url, connect_timeout=10)

    required = ("DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(
            "PostgreSQL audit configuration is missing: "
            + ", ".join(missing)
        )
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


class Audit:
    def __init__(self, connection_factory=None, stale_after_minutes=30):
        self.connection_factory = connection_factory or _connection_factory
        if stale_after_minutes <= 0:
            raise ValueError("stale_after_minutes must be positive")
        self.stale_after_minutes = stale_after_minutes

    def is_processed(self, source_gcs_path, event_id, source_checksum):
        return source_gcs_path in self.processed_paths(
            [
                {
                    "gcs_path": source_gcs_path,
                    "event_id": event_id,
                    "source_checksum": source_checksum,
                }
            ]
        )

    def processed_paths(self, source_files):
        if not source_files:
            return set()
        for source_file in source_files:
            if not source_file.get("gcs_path") or not source_file.get(
                "source_checksum"
            ):
                raise ValueError(
                    "Source path and checksum are required for audit lookup"
                )
        candidates = json.dumps(
            [
                {
                    "gcs_path": item["gcs_path"],
                    "event_id": item.get("event_id"),
                    "source_checksum": item["source_checksum"],
                }
                for item in source_files
            ]
        )
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH candidates AS (
                        SELECT *
                        FROM jsonb_to_recordset(%s::jsonb) AS candidate(
                            gcs_path text,
                            event_id text,
                            source_checksum char(64)
                        )
                    )
                    SELECT candidate.gcs_path
                    FROM candidates AS candidate
                    JOIN silver_ingestion_audit AS audit
                      ON audit.source_gcs_path = candidate.gcs_path
                     AND audit.source_checksum = candidate.source_checksum
                     AND audit.event_id IS NOT DISTINCT FROM candidate.event_id
                    WHERE audit.status = 'SUCCESS'
                       OR (
                           audit.status = 'PROCESSING'
                           AND audit.updated_at >= CURRENT_TIMESTAMP
                               - (%s * INTERVAL '1 minute')
                       )
                    """,
                    (candidates, self.stale_after_minutes),
                )
                return {row[0] for row in cursor.fetchall()}
        finally:
            connection.close()

    def claim(self, source_file, target_table):
        return self.claim_files([source_file], target_table).get(
            source_file["gcs_path"]
        )

    def claim_files(self, source_files, target_table):
        if not source_files:
            return {}
        candidates = json.dumps(
            [
                {
                    "gcs_path": item["gcs_path"],
                    "event_id": item.get("event_id"),
                    "event_type": item.get("event_type"),
                    "source_checksum": item["source_checksum"],
                }
                for item in source_files
            ]
        )
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH candidates AS (
                        SELECT *
                        FROM jsonb_to_recordset(%s::jsonb) AS candidate(
                            gcs_path text,
                            event_id text,
                            event_type text,
                            source_checksum char(64)
                        )
                    )
                    INSERT INTO silver_ingestion_audit (
                        source_gcs_path, event_id, event_type, source_checksum,
                        target_table, status, attempt_count, started_at,
                        completed_at, error_message, updated_at
                    )
                    SELECT gcs_path, event_id, event_type, source_checksum,
                           %s, 'PROCESSING', 1, CURRENT_TIMESTAMP, NULL, NULL,
                           CURRENT_TIMESTAMP
                    FROM candidates
                    WHERE true
                    ON CONFLICT (source_gcs_path) DO UPDATE
                    SET event_id = EXCLUDED.event_id,
                        event_type = EXCLUDED.event_type,
                        source_checksum = EXCLUDED.source_checksum,
                        target_table = EXCLUDED.target_table,
                        status = 'PROCESSING',
                        attempt_count = silver_ingestion_audit.attempt_count + 1,
                        started_at = CURRENT_TIMESTAMP,
                        completed_at = NULL,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE silver_ingestion_audit.status = 'FAILED'
                       OR silver_ingestion_audit.source_checksum
                            <> EXCLUDED.source_checksum
                       OR (
                           silver_ingestion_audit.status = 'PROCESSING'
                           AND silver_ingestion_audit.updated_at
                               < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
                       )
                    RETURNING source_gcs_path, ingestion_id
                    """,
                    (
                        candidates,
                        target_table,
                        self.stale_after_minutes,
                    ),
                )
                rows = cursor.fetchall()
            connection.commit()
            return {row[0]: row[1] for row in rows}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def complete(self, ingestion_id, rows_read, rows_written):
        self._finish(
            ingestion_id,
            status="SUCCESS",
            rows_read=rows_read,
            rows_written=rows_written,
            error_message=None,
        )

    def complete_files(self, records):
        self._finish_files(records, status="SUCCESS", error_message=None)

    def fail(self, ingestion_id, error):
        self._finish(
            ingestion_id,
            status="FAILED",
            rows_read=None,
            rows_written=None,
            error_message=str(error),
        )

    def fail_files(self, ingestion_ids, error):
        self._finish_files(
            [{"ingestion_id": ingestion_id} for ingestion_id in ingestion_ids],
            status="FAILED",
            error_message=str(error),
        )

    def _finish_files(self, records, *, status, error_message):
        if not records:
            return
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH records AS (
                        SELECT *
                        FROM jsonb_to_recordset(%s::jsonb) AS record(
                            ingestion_id bigint,
                            rows_read bigint,
                            rows_written bigint
                        )
                    )
                    UPDATE silver_ingestion_audit AS audit
                    SET status = %s,
                        rows_read = records.rows_read,
                        rows_written = records.rows_written,
                        completed_at = CURRENT_TIMESTAMP,
                        error_message = %s,
                        updated_at = CURRENT_TIMESTAMP
                    FROM records
                    WHERE audit.ingestion_id = records.ingestion_id
                      AND audit.status = 'PROCESSING'
                    RETURNING audit.ingestion_id
                    """,
                    (
                        json.dumps(records),
                        status,
                        error_message,
                    ),
                )
                updated_ids = {row[0] for row in cursor.fetchall()}
                expected_ids = {record["ingestion_id"] for record in records}
                if updated_ids != expected_ids:
                    raise RuntimeError(
                        "Could not update every Silver audit claim; "
                        f"expected {len(expected_ids)}, updated {len(updated_ids)}"
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _finish(
        self,
        ingestion_id,
        *,
        status,
        rows_read,
        rows_written,
        error_message,
    ):
        connection = self.connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE silver_ingestion_audit
                    SET status = %s,
                        rows_read = %s,
                        rows_written = %s,
                        completed_at = %s,
                        error_message = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ingestion_id = %s
                      AND status = 'PROCESSING'
                    """,
                    (
                        status,
                        rows_read,
                        rows_written,
                        datetime.now(timezone.utc),
                        error_message,
                        ingestion_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        f"Audit claim {ingestion_id} is missing or not PROCESSING"
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
