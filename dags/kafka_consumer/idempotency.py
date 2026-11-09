import hashlib

from kafka_consumer.audit import AuditRepository
from kafka_consumer.storage import GcsObjectStore


class IdempotencyService:
    def __init__(self, audit_repository, object_store):
        self.audit = audit_repository
        self.object_store = object_store

    @classmethod
    def from_clients(cls, connection_factory, bucket, timeout_seconds):
        return cls(
            AuditRepository(connection_factory),
            GcsObjectStore(bucket, timeout_seconds),
        )

    def process_valid(self, event, retry_count):
        return self.audit.process_success(event, self, retry_count)

    def process_invalid(self, invalid_event, retry_count):
        path = self.audit.record_quarantine(
            invalid_event,
            self,
            retry_count,
        )
        return "QUARANTINED", path

    def write_event(self, event):
        content = event.bronze_bytes()
        digest = hashlib.sha256(content).hexdigest()
        object_name = self.object_store.raw_object_name(event)
        self.object_store.write_once(
            object_name,
            content,
            {
                "event_id": event.event_id,
                "payload_sha256": event.payload_sha256,
                "sha256": digest,
            },
        )
        return f"gs://{self.object_store.bucket.name}/{object_name}"

    def quarantine_conflict(self, event):
        content = event.bronze_bytes()
        digest = hashlib.sha256(content).hexdigest()
        object_name = self.object_store.quarantine_object_name(
            f"{event.topic}:{event.partition}:{event.offset}",
            digest,
        )
        self.object_store.write_once(
            object_name,
            content,
            {
                "event_id": event.event_id,
                "payload_sha256": event.payload_sha256,
                "sha256": digest,
            },
        )
        return f"gs://{self.object_store.bucket.name}/{object_name}"

    def write_quarantine(self, source_key, data, digest):
        object_name = self.object_store.quarantine_object_name(source_key, digest)
        self.object_store.write_once(
            object_name,
            data,
            {"sha256": digest},
        )
        return f"gs://{self.object_store.bucket.name}/{object_name}"

    @staticmethod
    def sha256(data):
        return hashlib.sha256(data).hexdigest()
