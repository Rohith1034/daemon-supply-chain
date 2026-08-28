from datetime import datetime, timezone
import random

from psycopg2.extras import Json

from simulator.DB import get_connection
from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_started_task(cursor):

    cursor.execute(
        """
        SELECT
            id,
            payload

        FROM event_outbox

        WHERE event_type='TaskStarted'

        AND status='PENDING'

        ORDER BY created_at

        LIMIT 1

        FOR UPDATE SKIP LOCKED
        """
    )

    return cursor.fetchone()



def complete_task():


    conn = get_connection()
    cursor = conn.cursor()


    try:


        task = get_started_task(cursor)


        if not task:

            print(
                "No running task found"
            )

            return



        source_event_id = task[0]
        payload = task[1]


        task_id = payload["task_id"]

        worker_id = payload["worker_id"]



        now = datetime.now(
            timezone.utc
        )



        # Simulate labor metrics

        duration_minutes = random.randint(
            30,
            180
        )


        quantity_processed = random.randint(
            200,
            5000
        )


        accuracy = round(
            random.uniform(
                95,
                100
            ),
            2
        )


        damage_count = random.randint(
            0,
            5
        )



        event = {


            "event_id":
                generate_event_id(),


            "event_type":
                "TaskCompleted",


            "event_version":
                "1.0",


            "timestamp":
                now.isoformat(),


            "source":
                "labor-management-system",


            "aggregate_type":
                "WAREHOUSE_TASK",


            "aggregate_id":
                task_id,


            "correlation_id":
                generate_correlation_id(
                    task_id,
                    prefix="TASK"
                ),



            "task":{


                "task_id":
                    task_id,


                "worker_id":
                    worker_id,


                "status":
                    "COMPLETED",


                "completed_at":
                    now.isoformat()

            },


            "performance":{


                "duration_minutes":
                    duration_minutes,


                "quantity_processed":
                    quantity_processed,


                "accuracy_percent":
                    accuracy,


                "damage_count":
                    damage_count

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

        VALUES

        (%s,%s,%s,%s,%s,%s)

        """,

        (

        event["event_id"],

        event["event_type"],

        "WAREHOUSE_TASK",

        task_id,

        task_id,

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


        print(
            "Task completed:",
            task_id
        )


        return event



    except Exception as e:

        conn.rollback()
        raise e



    finally:

        cursor.close()
        conn.close()



if __name__=="__main__":

    complete_task()