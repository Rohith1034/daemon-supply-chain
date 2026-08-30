from datetime import datetime, timezone

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME="PackingTaskStarted"



def generate_packing_task_started(task_id):

    with Database() as db:


        task = db.fetch_one(
            """
            SELECT

                task_id,
                task_type,
                warehouse_id,
                assigned_worker_id,
                correlation_id,
                status

            FROM warehouse_tasks

            WHERE task_id=%s

            AND task_type='PACKING'

            FOR UPDATE

            """,
            (
                task_id,
            )
        )


        if not task:

            raise Exception(
                "Packing task not found"
            )


        if task["status"]!="CREATED":

            raise Exception(
                f"""
Packing task must be CREATED

CURRENT:
{task["status"]}
"""
            )



        now=datetime.now(
            timezone.utc
        )



        db.execute(
            """
            UPDATE warehouse_tasks

            SET

                status='STARTED',

                task_started_at=%s


            WHERE task_id=%s

            """,
            (
                now,
                task_id
            )
        )



        payload={

            "eventType":
                EVENT_NAME,

            "occurredAt":
                now.isoformat(),

            "packingTask":{

                "taskId":
                    task_id,

                "warehouseId":
                    task["warehouse_id"],

                "workerId":
                    task["assigned_worker_id"],

                "status":
                    "STARTED"

            },

            "correlationId":
                str(task["correlation_id"])

        }



        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="WAREHOUSE_TASK",

            aggregate_id=task_id,

            correlation_id=str(
                task["correlation_id"]
            ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

                "task_id":
                    task_id,

                "worker_id":
                    task["assigned_worker_id"]

            }

        )



if __name__=="__main__":

    try:

        import sys

        generate_packing_task_started(
            sys.argv[1]
        )


    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise