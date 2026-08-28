import json
from pathlib import Path

from psycopg2.extras import execute_values, Json
from tqdm import tqdm

from simulator.DB import get_connection

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_FILE = BASE_DIR / "output" / "warehouses.json"

BATCH_SIZE = 1000

INSERT_QUERY = """
INSERT INTO warehouses
(
    warehouse_id,
    warehouse_code,
    warehouse_name,
    warehouse_type,
    status,

    country,
    state,
    state_code,
    city,
    postal_code,
    latitude,
    longitude,
    timezone,

    total_area_sqft,
    storage_capacity_units,
    dock_doors,
    receiving_docks,
    shipping_docks,
    operating_hours,
    working_shifts,

    supported_storage_types,
    temperature_controlled,
    temperature_min_c,
    temperature_max_c,
    hazmat_certified,
    fragile_goods_supported,
    perishable_goods_supported,

    fulfillment_types,
    receiving_enabled,
    putaway_enabled,
    cross_docking_enabled,
    returns_processing_enabled,
    cycle_count_enabled,

    automation_level,
    conveyor_system,
    automated_storage,
    robotics_enabled,
    warehouse_management_system,

    daily_throughput_units,
    average_dock_to_stock_hours,
    inventory_accuracy_percent,
    on_time_processing_percent
)
VALUES %s
ON CONFLICT (warehouse_id) DO NOTHING;
"""


def load_json():
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def build_row(w):

    address = w.get("address", {})
    facility = w.get("facility", {})
    storage = w.get("storage", {})
    operations = w.get("operations", {})
    automation = w.get("automation", {})
    performance = w.get("performance", {})

    return (

        w["warehouse_id"],
        w["warehouse_code"],
        w["warehouse_name"],
        w["warehouse_type"],
        w["status"],

        address.get("country"),
        address.get("state"),
        address.get("state_code"),
        address.get("city"),
        address.get("postal_code"),
        address.get("latitude"),
        address.get("longitude"),
        address.get("timezone"),

        facility.get("total_area_sqft"),
        facility.get("storage_capacity_units"),
        facility.get("dock_doors"),
        facility.get("receiving_docks"),
        facility.get("shipping_docks"),
        facility.get("operating_hours"),
        facility.get("working_shifts"),

        Json(storage.get("supported_storage_types", [])),
        storage.get("temperature_controlled"),
        storage.get("temperature_min_c"),
        storage.get("temperature_max_c"),
        storage.get("hazmat_certified"),
        storage.get("fragile_goods_supported"),
        storage.get("perishable_goods_supported"),

        Json(operations.get("fulfillment_types", [])),
        operations.get("receiving_enabled"),
        operations.get("putaway_enabled"),
        operations.get("cross_docking_enabled"),
        operations.get("returns_processing_enabled"),
        operations.get("cycle_count_enabled"),

        automation.get("automation_level"),
        automation.get("conveyor_system"),
        automation.get("automated_storage"),
        automation.get("robotics_enabled"),
        automation.get("warehouse_management_system"),

        performance.get("daily_throughput_units"),
        performance.get("average_dock_to_stock_hours"),
        performance.get("inventory_accuracy_percent"),
        performance.get("on_time_processing_percent")

    )


def main():

    warehouses = load_json()

    conn = get_connection()
    cur = conn.cursor()

    batch = []
    inserted = 0

    try:

        for warehouse in tqdm(warehouses, desc="Loading Warehouses"):

            batch.append(build_row(warehouse))

            if len(batch) >= BATCH_SIZE:

                execute_values(
                    cur,
                    INSERT_QUERY,
                    batch,
                    page_size=BATCH_SIZE
                )

                conn.commit()

                inserted += len(batch)
                batch.clear()

        if batch:

            execute_values(
                cur,
                INSERT_QUERY,
                batch,
                page_size=BATCH_SIZE
            )

            conn.commit()

            inserted += len(batch)

        print("=" * 60)
        print(f"Successfully inserted {inserted} warehouses.")
        print("=" * 60)

    except Exception as e:

        conn.rollback()
        print("Error:", e)

    finally:

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()