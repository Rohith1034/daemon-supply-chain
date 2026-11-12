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


def _can_fit_putaway_items(locations, existing_inventory, items):
    remaining_capacity = {
        location["location_id"]: max(
            int(location["remaining_capacity"] or 0),
            0,
        )
        for location in locations
    }

    planned_items = []
    for item in items:
        inventory = existing_inventory.get(item["product_id"])
        quantity = int(item["quantity"])
        previous_quantity = (
            int(inventory["available_quantity"] or 0)
            if inventory
            else 0
        )
        current_location_id = (
            inventory.get("location_id")
            if inventory
            else None
        )
        if current_location_id and current_location_id not in remaining_capacity:
            return False
        current_capacity = remaining_capacity.get(current_location_id, 0)
        requires_relocation = bool(
            previous_quantity
            and (
                not current_location_id
                or current_location_id not in remaining_capacity
                or current_capacity < quantity
            )
        )
        required_capacity = (
            previous_quantity + quantity
            if requires_relocation or not current_location_id
            else quantity
        )
        planned_items.append(
            (
                required_capacity,
                quantity,
                previous_quantity,
                current_location_id,
                requires_relocation,
            )
        )

    for (
        required_capacity,
        quantity,
        previous_quantity,
        current_location_id,
        requires_relocation,
    ) in sorted(
        planned_items,
        key=lambda planned_item: planned_item[0],
        reverse=True,
    ):
        if (
            current_location_id in remaining_capacity
            and not requires_relocation
            and remaining_capacity[current_location_id] >= quantity
        ):
            remaining_capacity[current_location_id] -= quantity
            continue

        candidates = [
            (location_id, capacity)
            for location_id, capacity in remaining_capacity.items()
            if location_id != current_location_id
            and capacity >= required_capacity
        ]
        if not candidates:
            return False

        replacement_id, replacement_capacity = min(
            candidates,
            key=lambda candidate: candidate[1],
        )
        remaining_capacity[replacement_id] = (
            replacement_capacity - required_capacity
        )
        if (
            requires_relocation
            and current_location_id in remaining_capacity
        ):
            remaining_capacity[current_location_id] += previous_quantity

    return True


def get_warehouse_with_putaway_capacity(db, required_quantities):
    required_items = [
        {
            "product_id": item["product_id"],
            "quantity": int(item["quantity"]),
        }
        for item in required_quantities
    ]
    product_ids = list({item["product_id"] for item in required_items})
    warehouses = db.fetch_all(
        """
        SELECT
            w.warehouse_id,
            w.warehouse_code,
            w.warehouse_name,
            w.warehouse_type,
            w.status,
            wl.location_id,
            wl.capacity_units - COALESCE(wl.current_utilization, 0)
                AS remaining_capacity
        FROM warehouses w
        JOIN warehouse_locations wl
            ON wl.warehouse_id = w.warehouse_id
        WHERE w.status = 'ACTIVE'
          AND wl.status = 'ACTIVE'
          AND wl.capacity_units IS NOT NULL
        ORDER BY w.warehouse_id, remaining_capacity DESC
        """
    )
    inventory_rows = (
        db.fetch_all(
            """
            SELECT
                warehouse_id,
                product_id,
                location_id,
                COALESCE(
                    available_quantity,
                    on_hand_quantity - reserved_quantity - damaged_quantity,
                    0
                ) AS available_quantity
            FROM inventory
            WHERE product_id = ANY(%s)
            """,
            (product_ids,),
        )
        if product_ids
        else []
    )
    inventory_by_warehouse = {
        (row["warehouse_id"], row["product_id"]): row
        for row in inventory_rows
    }
    locations_by_warehouse = {}
    warehouse_by_id = {}
    for row in warehouses:
        warehouse_id = row["warehouse_id"]
        warehouse_by_id[warehouse_id] = {
            key: row[key]
            for key in ("warehouse_id", "warehouse_code", "warehouse_name", "warehouse_type", "status")
        }
        locations_by_warehouse.setdefault(warehouse_id, []).append(
            {
                "location_id": row["location_id"],
                "remaining_capacity": row["remaining_capacity"],
            }
        )

    eligible = [
        warehouse_by_id[warehouse_id]
        for warehouse_id, locations in locations_by_warehouse.items()
        if _can_fit_putaway_items(
            locations,
            {
                item["product_id"]: inventory_by_warehouse[
                    (warehouse_id, item["product_id"])
                ]
                for item in required_items
                if (warehouse_id, item["product_id"]) in inventory_by_warehouse
            },
            required_items,
        )
    ]
    if not eligible:
        raise RuntimeError(
            "No active warehouse has putaway locations with enough free capacity "
            f"for item quantities {required_items}"
        )
    return random.choice(eligible)



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