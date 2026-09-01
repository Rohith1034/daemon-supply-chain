from core.config import ID_PADDING_LENGTH


def format_id(prefix: str, number: int):
    """
    Convert numeric sequence value into business ID.

    Example:
    PO + 1

    becomes

    PO-000000001
    """

    return (
        f"{prefix}-"
        f"{str(number).zfill(ID_PADDING_LENGTH)}"
    )



def next_id(db, sequence_name, prefix):
    """
    Generic sequence based ID generator.
    """

    result = db.fetch_one(
        f"""
        SELECT nextval('{sequence_name}')
        """
    )

    number = result["nextval"]

    return format_id(
        prefix,
        number
    )



# ==========================
# Purchase Order
# ==========================

def next_purchase_order_id(db):

    return next_id(
        db,
        "po_id_seq",
        "PO"
    )



# ==========================
# Shipment
# ==========================

def next_shipment_id(db):

    return next_id(
        db,
        "shipment_id_seq",
        "SHIP"
    )



# ==========================
# Order
# ==========================

def next_order_id(db):

    return next_id(
        db,
        "order_id_seq",
        "ORD"
    )



# ==========================
# Payment
# ==========================

def next_payment_id(db):

    return next_id(
        db,
        "payment_id_seq",
        "PAY"
    )



# ==========================
# Warehouse Task
# ==========================

def next_task_id(db):

    return next_id(
        db,
        "task_id_seq",
        "TASK"
    )



# ==========================
# Inventory Allocation
# ==========================

def next_allocation_id(db):

    return next_id(
        db,
        "allocation_id_seq",
        "ALLOC"
    )



# ==========================
# Driver
# ==========================

def next_driver_id(db):

    return next_id(
        db,
        "driver_id_seq",
        "DRV"
    )



# ==========================
# Trailer
# ==========================

def next_trailer_id(db):

    return next_id(
        db,
        "trailer_id_seq",
        "TRL"
    )



# ==========================
# Correlation
# ==========================

def next_correlation_id(db):

    return next_id(
        db,
        "correlation_id_seq",
        "CORR"
    )

def next_checkpoint_id(db):

    result = db.fetch_one(
        """
        SELECT COUNT(*) + 1 AS id
        FROM shipment_checkpoints
        """
    )

    return (
        f"CHK{int(result['id']):08d}"
    )

def next_worker_shift_id(db):

    return next_id(
        db,
        "worker_shift_id_seq",
        "SHIFT"
    )


def next_task_event_id(db):

    return next_id(
        db,
        "task_event_id_seq",
        "TE"
    )

def next_inventory_transaction_id(db):

    return next_id(
        db,
        "inventory_transaction_id_seq",
        "INVTX"
    )



def next_worker_productivity_id(db):

    return next_id(
        db,
        "worker_productivity_id_seq",
        "WP"
    )

def next_adjustment_id(db):

    row = db.fetch_one(
        """
        SELECT nextval('adjustment_id_seq') AS id
        """
    )

    return f"ADJ-{int(row['id']):09d}"


def next_fulfillment_id(db):

    return next_id(
        db,
        "fulfillment_id_seq",
        "FUL"
    )

def next_package_id(db):

    return next_id(
        db,
        "package_id_seq",
        "PKG"
    )

import re


# =====================================================
# GENERIC DATABASE-BACKED NUMERIC ID GENERATOR
# =====================================================

def _next_id_from_table(
    db,
    table_name,
    column_name,
    prefix,
    width=9
):
    """
    Generate the next identifier from the existing rows
    in PostgreSQL.

    Example:

        LOAD-000000001
        LOAD-000000002

    The database is treated as the source of truth, so the
    value does not reset when the Python process restarts.
    """

    query = f"""
        SELECT
            {column_name}
        FROM {table_name}
        WHERE {column_name} LIKE %s
        ORDER BY {column_name} DESC
        LIMIT 1
        FOR UPDATE
    """

    last_id = db.fetch_one(
        query,
        (
            f"{prefix}-%",
        )
    )

    if not last_id:

        next_number = 1

    else:

        current_id = last_id[
            column_name
        ]

        match = re.search(
            rf"{re.escape(prefix)}-(\d+)$",
            current_id
        )

        if not match:

            next_number = 1

        else:

            next_number = (
                int(match.group(1)) + 1
            )

    return (
        f"{prefix}-{next_number:0{width}d}"
    )


# =====================================================
# LOADING ID
# =====================================================

def next_loading_id(db):
    """
    Generate the next outbound loading identifier.
    """

    return _next_id_from_table(
        db=db,
        table_name="outbound_shipment_loading_events",
        column_name="loading_id",
        prefix="LOAD",
        width=9
    )


# =====================================================
# TRACKING ID
# =====================================================

def next_tracking_id(db):
    """
    Generate the next outbound tracking identifier.
    """

    return _next_id_from_table(
        db=db,
        table_name="outbound_shipment_tracking",
        column_name="tracking_id",
        prefix="TRACK",
        width=9
    )