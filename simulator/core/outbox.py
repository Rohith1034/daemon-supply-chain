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


    db.execute(

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

        VALUES
        (
            %s,%s,%s,%s,%s,%s
        )

        RETURNING id

        """,

        (

            event_id,

            event_type,

            aggregate_type,

            aggregate_id,

            correlation_id,

            Json(payload)

        )

    )


    return event_id