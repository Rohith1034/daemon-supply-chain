from datetime import datetime,timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def start_task():

    conn=get_connection()
    cursor=conn.cursor()


    cursor.execute(
    """
    SELECT id, payload
    FROM event_outbox

    WHERE event_type='WorkerAssigned'

    AND status='PENDING'

    ORDER BY created_at

    LIMIT 1

    FOR UPDATE SKIP LOCKED

    """
    )


    data_row=cursor.fetchone()

    if not data_row:
        print("No worker assignment found")
        cursor.close()
        conn.close()
        return

    source_event_id=data_row[0]
    data=data_row[1]


    now=datetime.now(timezone.utc)


    event={

    "event_id":
        generate_event_id(),

    "event_type":
        "TaskStarted",

    "timestamp":
        now.isoformat(),

    "correlation_id":
        generate_correlation_id(
            data["task_id"],
            prefix="TASK"
        ),


    "task_id":
        data["task_id"],


    "worker_id":
        data["worker"]["worker_id"],


    "started_at":
        now.isoformat()

    }



    cursor.execute(
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
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
    )

    """,
    (
    event["event_id"],
    "TaskStarted",
    "TASK",
    event["task_id"],
    event["correlation_id"],
    Json(event)
    ))

    cursor.execute(
        """
        UPDATE event_outbox
        SET status='PROCESSED'
        WHERE id=%s
        """,
        (source_event_id,)
    )

    conn.commit()

    print("Started")


if __name__=="__main__":
    start_task()