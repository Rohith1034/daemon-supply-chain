import random


# ==================================
# Supplier Selection
# ==================================

def get_active_supplier(db):
    """
    Select an active supplier.

    Weighted by supplier rating.
    Higher rated suppliers get more orders.
    """

    suppliers = db.fetch_all(
        """
        SELECT *
        FROM suppliers
        WHERE status = 'ACTIVE'
        """
    )

    if not suppliers:
        raise Exception(
            "No active suppliers found"
        )


    weights = [
        float(
            supplier["rating"] or 1
        )
        for supplier in suppliers
    ]


    return random.choices(
        suppliers,
        weights=weights,
        k=1
    )[0]



# ==================================
# Supplier Products
# ==================================

def get_supplier_products(
        db,
        supplier_id,
        limit=8
):
    """
    Get products belonging to supplier.
    """

    products = db.fetch_all(
        """
        SELECT *
        FROM products
        WHERE supplier_id=%s
        AND status='ACTIVE'
        ORDER BY random()
        LIMIT %s
        """,
        (
            supplier_id,
            limit
        )
    )


    if not products:
        raise Exception(
            f"No products found for supplier {supplier_id}"
        )


    return products



# ==================================
# Warehouse Selection
# ==================================

def get_active_warehouse(db):
    """
    Select active warehouse.
    """

    warehouses = db.fetch_all(
        """
        SELECT *
        FROM warehouses
        WHERE status='ACTIVE'
        """
    )


    if not warehouses:
        raise Exception(
            "No active warehouses found"
        )


    return random.choice(
        warehouses
    )



# ==================================
# Customer Selection
# ==================================

def get_customer(db):

    customer = db.fetch_one(
        """
        SELECT *
        FROM customers
        ORDER BY random()
        LIMIT 1
        """
    )


    if not customer:
        raise Exception(
            "No customers found"
        )


    return customer



# ==================================
# Worker Selection
# ==================================

def get_available_worker(
        db,
        warehouse_id
):

    workers = db.fetch_all(
        """
        SELECT *
        FROM workers
        WHERE warehouse_id=%s
        AND LOWER(employment_status)='active'
        AND (
            current_status='AVAILABLE'
            OR current_status IS NULL
        )
        """,
        (
            warehouse_id,
        )
    )


    if not workers:
        raise Exception(
            f"No available workers for warehouse {warehouse_id}"
        )


    return random.choice(
        workers
    )


# ==================================
# Warehouse Location
# ==================================

def get_storage_location(
        db,
        warehouse_id
):

    locations = db.fetch_all(
        """
        SELECT *
        FROM warehouse_locations
        WHERE warehouse_id=%s
        AND status='ACTIVE'
        """,
        (
            warehouse_id,
        )
    )


    if not locations:
        raise Exception(
            "No warehouse locations found"
        )


    return random.choice(
        locations
    )



# ==================================
# Inventory
# ==================================

def get_inventory(
        db,
        product_id,
        warehouse_id
):

    inventory = db.fetch_one(
        """
        SELECT *
        FROM inventory
        WHERE product_id=%s
        AND warehouse_id=%s
        """,
        (
            product_id,
            warehouse_id
        )
    )


    return inventory



# ==================================
# Purchase Order
# ==================================

def get_purchase_order(
        db,
        po_id
):

    return db.fetch_one(
        """
        SELECT *
        FROM purchase_orders
        WHERE po_id=%s
        """,
        (
            po_id,
        )
    )



# ==================================
# Shipment
# ==================================

def get_shipment(
        db,
        shipment_id
):

    return db.fetch_one(
        """
        SELECT *
        FROM shipments
        WHERE shipment_id=%s
        """,
        (
            shipment_id,
        )
    )



# ==================================
# Order
# ==================================

def get_order(
        db,
        order_id
):

    return db.fetch_one(
        """
        SELECT *
        FROM orders
        WHERE order_id=%s
        """,
        (
            order_id,
        )
    )



# ==================================
# Driver
# ==================================

def get_driver(db):

    driver = db.fetch_one(
        """
        SELECT *
        FROM drivers
        WHERE status='ACTIVE'
        ORDER BY random()
        LIMIT 1
        """
    )


    if not driver:
        raise Exception(
            "No active drivers"
        )


    return driver



# ==================================
# Trailer
# ==================================

def get_trailer(db):

    trailer = db.fetch_one(
        """
        SELECT *
        FROM trailers
        WHERE status='AVAILABLE'
        ORDER BY random()
        LIMIT 1
        """
    )


    if not trailer:
        raise Exception(
            "No available trailers"
        )


    return trailer

def get_worker_for_task(
    db,
    warehouse_id
):

    return db.fetch_one(
    """
    SELECT *
    FROM workers
    WHERE warehouse_id=%s
    AND status='AVAILABLE'
    ORDER BY random()
    LIMIT 1
    """,
    (warehouse_id,)
    )


def get_pending_received_shipment(db):

    shipment = db.fetch_one(
        """
        SELECT *
        FROM shipments
        WHERE shipment_status='ARRIVED'
        ORDER BY updated_at
        LIMIT 1
        """
    )


    if not shipment:
        raise Exception(
            "No arrived shipment found"
        )


    return shipment


def get_shipment_items(
        db,
        shipment_id
):

    items = db.fetch_all(
        """
        SELECT *
        FROM shipment_items
        WHERE shipment_id=%s
        """,
        (
            shipment_id,
        )
    )

    if not items:
        raise Exception(
            "No shipment items found"
        )

    return items

def get_arrived_shipment(db):

    shipment = db.fetch_one(
        """
        SELECT *
        FROM shipments
        WHERE shipment_status='ARRIVED'
        ORDER BY created_at
        LIMIT 1
        """
    )

    if not shipment:
        raise Exception(
            "No arrived shipment found"
        )

    return shipment


def get_shipment_items(
        db,
        shipment_id
):

    items = db.fetch_all(
        """
        SELECT *
        FROM shipment_items
        WHERE shipment_id=%s
        """,
        (
            shipment_id,
        )
    )

    if not items:
        raise Exception(
            "No shipment items found"
        )

    return items


def get_available_worker_for_task(
        db,
        warehouse_id,
        task_type
):

    workers = db.fetch_all(

        """
        SELECT *
        FROM workers
        WHERE warehouse_id=%s
        AND employment_status='Active'
        ORDER BY productivity_rating DESC
        LIMIT 10
        """,

        (
            warehouse_id,
        )
    )


    if not workers:

        raise Exception(
            "No active workers found"
        )


    return random.choice(
        workers
    )