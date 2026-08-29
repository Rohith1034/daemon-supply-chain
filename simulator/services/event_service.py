import uuid
from datetime import datetime

from DB import db


class EventService:
    """
    Responsible ONLY for writing events into event_outbox.

    It does not know anything about purchase orders,
    shipments,
    warehouse,
    inventory,
    etc.
    """

    def publish_event(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        correlation_id: str,
        payload: dict,
    ):

        sql = """
        INSERT INTO event_outbox
        (
            event_id,
            event_type,
            aggregate_type,
            aggregate_id,
            correlation_id,
            payload,
            status,
            created_at
        )
        VALUES
        (
            %s,%s,%s,%s,%s,%s,%s,%s
        )
        """

        params = (
            str(uuid.uuid4()),
            event_type,
            aggregate_type,
            aggregate_id,
            correlation_id,
            payload,
            "PENDING",
            datetime.utcnow(),
        )

        db.insert(sql, params)

    def mark_as_published(self, event_id):

        sql = """
        UPDATE event_outbox
        SET
            status='PUBLISHED',
            published_at=NOW()
        WHERE
            event_id=%s
        """

        db.update(sql, (event_id,))

    def mark_as_failed(self, event_id):

        sql = """
        UPDATE event_outbox
        SET
            status='FAILED'
        WHERE
            event_id=%s
        """

        db.update(sql, (event_id,))

    def get_pending_events(self):

        sql = """
        SELECT *
        FROM event_outbox
        WHERE status='PENDING'
        ORDER BY created_at
        """

        return db.fetch_all(sql)