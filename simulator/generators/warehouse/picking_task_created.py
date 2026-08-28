import random

from datetime import datetime, timezone

from psycopg2.extras import Json

from simulator.DB import get_connection

from services.event_service import (
    generate_event_id,
    generate_correlation_id
)



def get_allocated_order(cursor):

    cursor.execute(
        """
        SELECT
        allocation_id,
        order_id,
        warehouse_id

        FROM inventory_allocations

        WHERE allocation_status='ALLOCATED'

        ORDER BY random()

        LIMIT 1

        FOR UPDATE SKIP LOCKED

        """
    )

    return cursor.fetchone()



def get_allocation_items(cursor, order_id):

    cursor.execute(

        """

        SELECT

        product_id,
        allocated_quantity

        FROM inventory_allocations

        WHERE order_id=%s


        """,

        (
            order_id,
        )

    )


    return cursor.fetchall()



def get_worker(cursor, warehouse_id):

    cursor.execute(

        """

        SELECT

        worker_id,
        role,
        productivity_rating


        FROM workers


        WHERE

        warehouse_id=%s

        AND

        employment_status='Active'


        AND

        role IN

        (
        'Picker',
        'Inventory Clerk'
        )


        ORDER BY productivity_rating DESC


        LIMIT 1


        """,

        (
            warehouse_id,
        )

    )


    return cursor.fetchone()



def create_picking_task():

    conn=get_connection()

    cursor=conn.cursor()


    try:


        allocation=get_allocated_order(cursor)


        if not allocation:

            raise Exception(
                "No allocation found"
            )


        allocation_id=allocation[0]

        order_id=allocation[1]

        warehouse_id=allocation[2]



        items=get_allocation_items(

            cursor,

            order_id

        )



        worker=get_worker(

            cursor,

            warehouse_id

        )


        if not worker:

            raise Exception(
                "No worker available"
            )



        worker_id=worker[0]



        task_id=(

            f"TASK-{random.randint(1,99999999):08d}"

        )



        now=datetime.now(
            timezone.utc
        )



        total_quantity=sum(

            x[1]

            for x in items

        )



        cursor.execute(

        """

        INSERT INTO warehouse_tasks
        (
            task_id,
            task_type,
            warehouse_id,
            shipment_id,
            product_id,
            location,
            quantity,
            priority,
            status,
            assigned_worker_id,
            estimated_minutes,
            created_at
        )


        VALUES

        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

        """,

        (

        task_id,

        "PICKING",

        warehouse_id,

        None,

        items[0][0] if items else None,

        None,

        total_quantity,

        random.choice(

            [
            "NORMAL",
            "HIGH"
            ]

        ),

        "CREATED",

        worker_id,

        None,

        now

        )

        )



        correlation_id=(

            generate_correlation_id(
                order_id,
                prefix="ORDER"
            )

        )



        event={


        "event_id":

            generate_event_id(),


        "event_type":

            "PickingTaskCreated",


        "event_version":

            "1.0",


        "timestamp":

            now.isoformat(),


        "source":

            "warehouse-management-system",


        "aggregate_type":

            "WAREHOUSE_TASK",


        "aggregate_id":

            task_id,


        "correlation_id":

            correlation_id,


        "task":{


            "task_id":

                task_id,


            "order_id":

                order_id,


            "warehouse_id":

                warehouse_id,


            "worker_id":

                worker_id,


            "task_type":

                "PICKING",


            "status":

                "CREATED",


            "items":

                [

                {

                "product_id":x[0],

                "quantity":x[1]

                }

                for x in items

                ]

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

        correlation_id,

        Json(event)

        )

        )

        cursor.execute(
            """
            UPDATE inventory_allocations
            SET allocation_status='PICKING_CREATED'
            WHERE order_id=%s
            AND allocation_status='ALLOCATED'
            """,
            (order_id,)
        )



        conn.commit()


        print(
            "Picking task created:",
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

    create_picking_task()
