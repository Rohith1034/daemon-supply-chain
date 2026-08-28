from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)


def assign_worker():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT id, payload
            FROM event_outbox
            WHERE event_type IN ('WarehouseTaskCreated', 'PickingTaskCreated')
            AND status='PENDING'
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )

        task_event = cursor.fetchone()

        if not task_event:
            print("No warehouse task found")
            return

        source_event_id = task_event[0]
        task = task_event[1]["task"]

        cursor.execute(
            """
            SELECT worker_id, role, productivity_rating
            FROM workers
            WHERE warehouse_id=%s
            AND employment_status='Active'
            ORDER BY productivity_rating DESC
            LIMIT 1
            """,
            (task["warehouse_id"],)
        )

        worker = cursor.fetchone()

        if not worker:
            raise Exception("No worker available")

        event = {
            "event_id": generate_event_id(),
            "event_type": "WorkerAssigned",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": generate_correlation_id(
                task["task_id"],
                prefix="TASK"
            ),
            "task_id": task["task_id"],
            "worker": {
                "worker_id": worker[0],
                "role": worker[1],
                "productivity": worker[2]
            }
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
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                event["event_id"],
                event["event_type"],
                "WORKER",
                task["task_id"],
                event["correlation_id"],
                Json(event)
            )
        )

        cursor.execute(
            """
            UPDATE event_outbox
            SET status='PROCESSED'
            WHERE id=%s
            """,
            (source_event_id,)
        )

        conn.commit()

        print("Assigned:", worker[0])

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    assign_worker()
