from datetime import timedelta, timezone
import random
import uuid
import sys


from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "PackingCompleted"


# ============================================================
# DATETIME NORMALIZATION
# ============================================================

def _ensure_utc(value):
    """
    Normalize a datetime to timezone-aware UTC.
    """

    if value is None:

        return None

    if value.tzinfo is None:

        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )


# ============================================================
# PACKING COMPLETION TIME
# ============================================================

def _get_packing_completed_time(
    task,
    actual_minutes
):
    """
    Calculate a causally valid PackingCompleted timestamp.

    PackingCompleted must happen after PackingTaskStarted.

    Priority:
        1. task_started_at
        2. assigned_at
        3. created_at
        4. simulation clock
    """

    task_started_at = _ensure_utc(
        task.get(
            "task_started_at"
        )
    )

    assigned_at = _ensure_utc(
        task.get(
            "assigned_at"
        )
    )

    created_at = _ensure_utc(
        task.get(
            "created_at"
        )
    )

    simulation_now = _ensure_utc(
        get_simulation_now()
    )

    if task_started_at is not None:

        base_time = task_started_at

    elif assigned_at is not None:

        base_time = assigned_at

    elif created_at is not None:

        base_time = created_at

    else:

        base_time = simulation_now

    return (
        base_time
        +
        timedelta(
            minutes=actual_minutes
        )
    )


# ============================================================
# PACKAGE ID
# ============================================================

def next_package_id(db):
    """
    Generate the next sequential package ID.

    Uses the current maximum numeric package suffix rather
    than COUNT(*), so deleted rows do not cause collisions.
    """

    row = db.fetch_one(
        """
        SELECT
            COALESCE(
                MAX(
                    CAST(
                        SUBSTRING(
                            package_id
                            FROM 'PKG-([0-9]+)$'
                        )
                        AS BIGINT
                    )
                ),
                0
            ) AS max_id
        FROM packages
        WHERE package_id LIKE 'PKG-%'
        """
    )

    next_id = int(
        row["max_id"]
    ) + 1

    return (
        f"PKG-{next_id:09d}"
    )


# ============================================================
# FIND STARTED PACKING TASK
# ============================================================

def _get_packing_task(
    db,
    task_id=None
):
    """
    Find a STARTED packing task.

    When task_id is provided, the exact task is selected.
    Otherwise the oldest STARTED packing task is selected.
    """

    if task_id:

        return db.fetch_one(
            """
            SELECT
                *
            FROM warehouse_tasks
            WHERE task_id=%s
              AND task_type='PACKING'
              AND status='STARTED'
            LIMIT 1
            FOR UPDATE
            """,
            (
                task_id,
            )
        )

    return db.fetch_one(
        """
        SELECT
            *
        FROM warehouse_tasks
        WHERE task_type='PACKING'
          AND status='STARTED'
        ORDER BY task_started_at ASC
        LIMIT 1
        FOR UPDATE
        """
    )


# ============================================================
# FIND PICKING TASK
# ============================================================

def _get_picking_task(
    db,
    picking_task_id
):
    """
    Retrieve the exact picking task associated with the
    packing task.

    This provides the source of truth for the product and
    quantity that are being packed.
    """

    return db.fetch_one(
        """
        SELECT
            task_id,
            order_id,
            warehouse_id,
            assigned_worker_id,
            allocation_id,
            product_id,
            quantity,
            status,
            correlation_id,
            task_completed_at,
            completed_at
        FROM warehouse_tasks
        WHERE task_id=%s
          AND task_type='PICKING'
        LIMIT 1
        """,
        (
            picking_task_id,
        )
    )


# ============================================================
# CHECK WHETHER ALL PACKING TASKS ARE COMPLETE
# ============================================================

def _all_packing_tasks_completed(
    db,
    order_id
):
    """
    Return True when every packing task belonging to the
    order has been completed.
    """

    row = db.fetch_one(
        """
        SELECT
            COUNT(*) AS total_tasks,
            COUNT(
                CASE
                    WHEN status='COMPLETED'
                    THEN 1
                END
            ) AS completed_tasks
        FROM warehouse_tasks
        WHERE order_id=%s
          AND task_type='PACKING'
        """,
        (
            order_id,
        )
    )

    total_tasks = int(
        row["total_tasks"]
    )

    completed_tasks = int(
        row["completed_tasks"]
    )

    return (
        total_tasks > 0
        and
        total_tasks == completed_tasks
    )


# ============================================================
# PREVENT DUPLICATE PACKAGE FOR PACKING TASK
# ============================================================

def _get_existing_package_for_task(
    db,
    order_id,
    task_id
):
    """
    A packing task should create at most one package.

    Package-task relationship is inferred through package_items
    and the exact picked product.
    """

    return db.fetch_one(
        """
        SELECT DISTINCT
            p.package_id,
            p.order_id,
            p.warehouse_id,
            p.package_status,
            p.total_items,
            p.total_quantity,
            p.weight_kg,
            p.length_cm,
            p.width_cm,
            p.height_cm,
            p.packed_by,
            p.packed_at,
            p.correlation_id
        FROM packages p

        INNER JOIN package_items pi
            ON pi.package_id =
               p.package_id

        INNER JOIN warehouse_tasks wt
            ON wt.order_id =
               p.order_id
           AND wt.task_type='PACKING'

        WHERE p.order_id=%s
          AND wt.task_id=%s
          AND wt.status='COMPLETED'

        LIMIT 1
        """,
        (
            order_id,
            task_id
        )
    )


# ============================================================
# MAIN GENERATOR
# ============================================================

def generate_packing_completed(
    task_id=None
):

    with Database() as db:

        # ====================================================
        # 1. FIND STARTED PACKING TASK
        # ====================================================

        task = _get_packing_task(
            db,
            task_id
        )

        if not task:

            if task_id:

                raise Exception(
                    f"No STARTED packing task found "
                    f"for task_id={task_id}"
                )

            raise Exception(
                "No STARTED packing task found"
            )

        # ====================================================
        # 2. EXTRACT PACKING TASK DETAILS
        # ====================================================

        task_id = task[
            "task_id"
        ]

        order_id = task[
            "order_id"
        ]

        warehouse_id = task[
            "warehouse_id"
        ]

        worker_id = task[
            "assigned_worker_id"
        ]

        picking_task_id = task.get(
            "picking_task_id"
        )

        correlation_id = (
            str(
                task["correlation_id"]
            )
            if task["correlation_id"]
            else str(
                uuid.uuid4()
            )
        )

        # ====================================================
        # 3. VALIDATE PACKING TASK
        # ====================================================

        if not order_id:

            raise Exception(
                f"Packing task {task_id} "
                "is missing order_id"
            )

        if not warehouse_id:

            raise Exception(
                f"Packing task {task_id} "
                "is missing warehouse_id"
            )

        if not worker_id:

            raise Exception(
                f"Packing task {task_id} "
                "has no assigned worker"
            )

        if not picking_task_id:

            raise Exception(
                f"""
Packing task has no picking_task_id.

PACKING TASK:
{task_id}

ORDER:
{order_id}
"""
            )

        # ====================================================
        # 4. GET EXACT PICKING TASK
        # ====================================================

        picking_task = _get_picking_task(
            db,
            picking_task_id
        )

        if not picking_task:

            raise Exception(
                f"""
Picking task not found.

PICKING TASK:
{picking_task_id}

PACKING TASK:
{task_id}
"""
            )

        # ====================================================
        # 5. VALIDATE PICKING TASK RELATIONSHIP
        # ====================================================

        if picking_task[
            "order_id"
        ] != order_id:

            raise Exception(
                f"""
Packing/picking order mismatch.

PACKING TASK:
{task_id}

PACKING ORDER:
{order_id}

PICKING TASK:
{picking_task_id}

PICKING ORDER:
{picking_task["order_id"]}
"""
            )

        if picking_task[
            "warehouse_id"
        ] != warehouse_id:

            raise Exception(
                f"""
Packing/picking warehouse mismatch.

PACKING TASK:
{task_id}

PACKING WAREHOUSE:
{warehouse_id}

PICKING TASK:
{picking_task_id}

PICKING WAREHOUSE:
{picking_task["warehouse_id"]}
"""
            )

        if picking_task[
            "status"
        ] != "COMPLETED":

            raise Exception(
                f"""
Picking task is not completed.

PICKING TASK:
{picking_task_id}

STATUS:
{picking_task["status"]}
"""
            )

        # ====================================================
        # 6. EXTRACT EXACT ITEM
        #
        # IMPORTANT:
        #
        # Do NOT query every order_items row here.
        #
        # This packing task is associated with exactly one
        # picking task, which identifies the product and
        # quantity that this package contains.
        # ====================================================

        product_id = picking_task[
            "product_id"
        ]

        picked_quantity = picking_task[
            "quantity"
        ]

        if not product_id:

            raise Exception(
                f"""
Picking task has no product_id.

PICKING TASK:
{picking_task_id}
"""
            )

        if picked_quantity is None:

            raise Exception(
                f"""
Picking task has no quantity.

PICKING TASK:
{picking_task_id}
"""
            )

        picked_quantity = int(
            picked_quantity
        )

        if picked_quantity <= 0:

            raise Exception(
                f"""
Invalid picking quantity.

PICKING TASK:
{picking_task_id}

QUANTITY:
{picked_quantity}
"""
            )

        # ====================================================
        # 7. CHECK FOR EXISTING PACKAGE
        #
        # This protects against accidental duplicate package
        # generation for the same packing task.
        # ====================================================

        existing_package = (
            _get_existing_package_for_task(
                db,
                order_id,
                task_id
            )
        )

        if existing_package:

            raise Exception(
                f"""
A package already exists for this packing task.

PACKING TASK:
{task_id}

PACKAGE:
{existing_package["package_id"]}
"""
            )

        # ====================================================
        # 8. GENERATE PACKING DURATION
        # ====================================================

        actual_minutes = random.randint(
            10,
            45
        )

        # ====================================================
        # 9. CALCULATE PACKED TIMESTAMP
        # ====================================================

        packed_at = _get_packing_completed_time(
            task,
            actual_minutes
        )

        # ====================================================
        # 10. GENERATE PACKAGE ID
        # ====================================================

        package_id = next_package_id(
            db
        )

        # ====================================================
        # 11. PACKAGE DIMENSIONS
        # ====================================================

        weight_kg = round(
            random.uniform(
                1,
                10
            ),
            2
        )

        length_cm = 30

        width_cm = 20

        height_cm = 15

        # ====================================================
        # 12. CREATE PACKAGE
        #
        # ONE PACKING TASK = ONE PACKAGE
        #
        # The package contains only the item handled by
        # this picking task.
        # ====================================================

        db.execute(
            """
            INSERT INTO packages
            (
                package_id,
                order_id,
                warehouse_id,
                package_type,
                total_items,
                total_quantity,
                weight_kg,
                length_cm,
                width_cm,
                height_cm,
                package_status,
                packed_by,
                packed_at,
                correlation_id
            )
            VALUES
            (
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s
            )
            """,
            (
                package_id,
                order_id,
                warehouse_id,
                "BOX",
                1,
                picked_quantity,
                weight_kg,
                length_cm,
                width_cm,
                height_cm,
                "PACKED",
                worker_id,
                packed_at,
                correlation_id
            )
        )

        # ====================================================
        # 13. CREATE PACKAGE ITEM
        # ====================================================

        db.execute(
            """
            INSERT INTO package_items
            (
                package_id,
                product_id,
                quantity
            )
            VALUES
            (
                %s,%s,%s
            )
            """,
            (
                package_id,
                product_id,
                picked_quantity
            )
        )

        # ====================================================
        # 14. COMPLETE PACKING TASK
        # ====================================================

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
              AND task_type='PACKING'
              AND status='STARTED'
            """,
            (
                actual_minutes,
                packed_at,
                packed_at,
                worker_id,
                task_id
            )
        )

        # ====================================================
        # 15. VERIFY PACKING TASK REALLY COMPLETED
        # ====================================================

        completed_task = db.fetch_one(
            """
            SELECT
                status
            FROM warehouse_tasks
            WHERE task_id=%s
            LIMIT 1
            """,
            (
                task_id,
            )
        )

        if not completed_task:

            raise Exception(
                f"Packing task disappeared after update: "
                f"{task_id}"
            )

        if completed_task[
            "status"
        ] != "COMPLETED":

            raise Exception(
                f"""
Packing task failed to transition to COMPLETED.

TASK:
{task_id}

STATUS:
{completed_task["status"]}
"""
            )

        # ====================================================
        # 16. DETERMINE WHETHER ENTIRE ORDER IS PACKED
        #
        # DO NOT mark the order PACKED simply because this
        # individual packing task completed.
        # ====================================================

        all_packing_completed = (
            _all_packing_tasks_completed(
                db,
                order_id
            )
        )

        if all_packing_completed:

            db.execute(
                """
                UPDATE orders
                SET
                    order_status='PACKED'
                WHERE order_id=%s
                  AND order_status NOT IN
                      (
                          'DELIVERED',
                          'CANCELLED'
                      )
                """,
                (
                    order_id,
                )
            )

        # ====================================================
        # 17. RELEASE WORKER
        # ====================================================

        db.execute(
            """
            UPDATE workers
            SET
                current_status='AVAILABLE'
            WHERE worker_id=%s
            """,
            (
                worker_id,
            )
        )

        # ====================================================
        # 18. EVENT PAYLOAD
        # ====================================================

        payload = {

            "eventType":
                EVENT_NAME,

            "occurredAt":
                packed_at.isoformat(),

            "package":
            {

                "packageId":
                    package_id,

                "taskId":
                    task_id,

                "pickingTaskId":
                    picking_task_id,

                "orderId":
                    order_id,

                "warehouseId":
                    warehouse_id,

                "workerId":
                    worker_id,

                "productId":
                    product_id,

                "totalItems":
                    1,

                "totalQuantity":
                    picked_quantity,

                "weightKg":
                    weight_kg,

                "lengthCm":
                    length_cm,

                "widthCm":
                    width_cm,

                "heightCm":
                    height_cm,

                "packageType":
                    "BOX",

                "status":
                    "PACKED",

                "packedAt":
                    packed_at.isoformat()

            },

            "item":
            {

                "productId":
                    product_id,

                "quantity":
                    picked_quantity

            },

            "order":
            {

                "orderId":
                    order_id,

                "packingCompleted":
                    all_packing_completed,

                "status":
                    "PACKED"
                    if all_packing_completed
                    else "PACKING"

            },

            "correlationId":
                correlation_id

        }

        # ====================================================
        # 19. PUBLISH EVENT
        # ====================================================

        publish_event(
            db=db,

            event_type=EVENT_NAME,

            aggregate_type="PACKAGE",

            aggregate_id=package_id,

            correlation_id=correlation_id,

            payload=payload
        )

        # ====================================================
        # 20. LOG SUCCESS
        # ====================================================

        log_event_success(
            EVENT_NAME,
            {

                "package_id":
                    package_id,

                "task_id":
                    task_id,

                "picking_task_id":
                    picking_task_id,

                "order_id":
                    order_id,

                "warehouse_id":
                    warehouse_id,

                "worker_id":
                    worker_id,

                "product_id":
                    product_id,

                "quantity":
                    picked_quantity,

                "actual_minutes":
                    actual_minutes,

                "packed_at":
                    packed_at,

                "all_packing_completed":
                    all_packing_completed,

                "correlation_id":
                    correlation_id

            }
        )

        # ====================================================
        # 21. RETURN RESULT
        # ====================================================

        return {

            "package_id":
                package_id,

            "task_id":
                task_id,

            "picking_task_id":
                picking_task_id,

            "order_id":
                order_id,

            "product_id":
                product_id,

            "total_quantity":
                picked_quantity,

            "packed_at":
                packed_at,

            "order_packed":
                all_packing_completed

        }


# ============================================================
# CLI ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        if len(sys.argv) > 1:

            generate_packing_completed(
                sys.argv[1]
            )

        else:

            generate_packing_completed()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
