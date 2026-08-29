from datetime import datetime, timezone
import random


from core.db import Database

from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)


EVENT_NAME = "TaskCompleted"



def generate_task_completed():


    with Database() as db:


        # ---------------------------------
        # Find started receiving task
        # ---------------------------------

        task = db.fetch_one(
            """
            SELECT *
            FROM warehouse_tasks
            WHERE status='STARTED'
            ORDER BY task_started_at
            LIMIT 1
            """
        )


        if not task:
            raise Exception(
                "No STARTED warehouse task found"
            )


        completed_time = datetime.now(
            timezone.utc
        )


        worker_id = task["assigned_worker_id"]


        actual_minutes = random.randint(
            10,
            45
        )


        accuracy_score = round(
            random.uniform(95,100),
            2
        )


        productivity_score = round(
            random.uniform(85,100),
            2
        )



        # ---------------------------------
        # Update warehouse task
        # ---------------------------------

        db.execute(
            """
            UPDATE warehouse_tasks

            SET

            status='COMPLETED',

            actual_minutes=%s,

            completed_at=%s,

            task_completed_at=%s,

            completed_by=%s


            WHERE task_id=%s

            """,
            (
                actual_minutes,

                completed_time,

                completed_time,

                worker_id,

                task["task_id"]
            )
        )



        # ---------------------------------
        # Insert LMS productivity
        # ---------------------------------

        db.execute(
            """

            INSERT INTO worker_productivity
            (
                worker_id,
                task_type,
                units_processed,
                working_minutes,
                accuracy_score,
                productivity_score,
                task_id,
                warehouse_id,
                correlation_id
            )


            VALUES

            (
                %s,%s,%s,%s,%s,%s,%s,%s,%s
            )

            """,

            (

                worker_id,

                task["task_type"],

                task["quantity"],

                actual_minutes,

                accuracy_score,

                productivity_score,

                task["task_id"],

                task["warehouse_id"],

                task["correlation_id"]

            )
        )



        # ---------------------------------
        # Event payload
        # ---------------------------------

        payload = {


            "event_type":
                EVENT_NAME,


            "task_id":
                task["task_id"],


            "task_type":
                task["task_type"],


            "shipment_id":
                task["shipment_id"],


            "warehouse_id":
                task["warehouse_id"],


            "worker_id":
                worker_id,


            "status":
                "COMPLETED",


            "actual_minutes":
                actual_minutes,


            "units_processed":
                task["quantity"],


            "accuracy_score":
                accuracy_score,


            "productivity_score":
                productivity_score,


            "correlation_id":
                str(task["correlation_id"])

        }



        # ---------------------------------
        # Outbox
        # ---------------------------------

        publish_event(

            db=db,

            event_type=EVENT_NAME,

            aggregate_type="WAREHOUSE_TASK",

            aggregate_id=task["task_id"],

            correlation_id=str(
                task["correlation_id"]
            ),

            payload=payload

        )



        log_event_success(

            EVENT_NAME,

            {

                "task_id":
                    task["task_id"],


                "task_type":
                    task["task_type"],


                "shipment_id":
                    task["shipment_id"],


                "warehouse_id":
                    task["warehouse_id"],


                "worker_id":
                    worker_id,


                "actual_minutes":
                    actual_minutes,


                "productivity_score":
                    productivity_score,


                "correlation_id":
                    task["correlation_id"]

            }

        )





if __name__ == "__main__":


    try:

        generate_task_completed()


    except Exception as e:

        log_event_failure(

            EVENT_NAME,

            e

        )

        raise