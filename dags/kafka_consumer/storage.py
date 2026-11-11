import hashlib
from datetime import datetime, timezone
from urllib.parse import quote

from google.api_core.exceptions import Conflict, PreconditionFailed


class GcsObjectConflictError(RuntimeError):
    pass


class GcsObjectStore:
    def __init__(self, bucket, timeout_seconds=60):
        self.bucket = bucket
        self.timeout_seconds = timeout_seconds

    def write_once(self, object_name, data, metadata):
        blob = self.bucket.blob(object_name)
        blob.metadata = metadata
        try:
            blob.upload_from_string(
                data,
                content_type="application/json",
                if_generation_match=0,
                checksum="auto",
                timeout=self.timeout_seconds,
            )
            return True
        except (Conflict, PreconditionFailed):
            existing = self.bucket.blob(object_name)
            existing.reload(timeout=self.timeout_seconds)
            existing_metadata = existing.metadata or {}
            expected_sha256 = metadata["sha256"]
            if existing_metadata.get("sha256") == expected_sha256:
                return False
            existing_data = existing.download_as_bytes(
                timeout=self.timeout_seconds
            )
            if hashlib.sha256(existing_data).hexdigest() == expected_sha256:
                return False
            raise GcsObjectConflictError(
                f"Object {object_name!r} exists with different content"
            )

    @staticmethod
    def raw_object_name(event):
        topic = quote(event.topic, safe="")
        event_type = quote(event.event_type, safe="")
        event_id = quote(event.event_id, safe="")
        event_time = datetime.now(timezone.utc)
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        event_time = event_time.astimezone(timezone.utc)
        return (
            f"raw/{topic}/{event_type}/{event_time:%Y/%m/%d}/"
            f"hour={event_time:%H}/"
            f"ingestion_time={event_time:%Y%m%dT%H%M%S%f}/"
            f"event_{event_id}.json"
        )

    @staticmethod
    def quarantine_object_name(source_key, digest):
        topic, partition, offset = source_key.rsplit(":", 2)
        topic = quote(topic, safe="")
        return (
            f"quarantine/{topic}/partition={partition}/offset={offset}/"
            f"{digest}.json"
        )
