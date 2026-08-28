import json
import math
from pathlib import Path

from psycopg2.extras import execute_values
from tqdm import tqdm

from simulator.DB import get_connection

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_FILE = BASE_DIR / "output" / "products.json"

BATCH_SIZE = 7000


INSERT_QUERY = """
INSERT INTO products
(
    product_id,
    sku,
    barcode,
    name,
    description,

    category_level_1,
    category_level_2,
    category_level_3,

    brand,

    attributes_color,
    attributes_size,
    attributes_material,

    cost_price,
    selling_price,
    currency,

    supplier_id,

    storage_type,
    shelf_life_days,

    weight_kg,
    fragile,
    hazardous,

    status,

    created_at,
    updated_at
)
VALUES %s
ON CONFLICT (product_id) DO NOTHING;
"""


def load_json():
    """
    Handles JSON files containing NaN values.
    """

    with open(JSON_FILE, "r", encoding="utf-8") as f:

        return json.load(
            f,
            parse_constant=lambda x: None
        )


def clean_price(value):

    if value is None:
        return None

    if isinstance(value, (int, float)):
        return value

    return float(
        str(value)
        .replace("$", "")
        .replace(",", "")
        .strip()
    )


def clean_value(value):

    if isinstance(value, float):

        if math.isnan(value):
            return None

    return value


def build_row(p):

    return (

        p["product_id"],
        p["product_sku"],
        p["product_barcode"],

        p["name"],
        p.get("description"),

        p.get("category.level_1"),
        p.get("category.level_2"),
        p.get("category.level_3"),

        p.get("brand"),

        clean_value(p.get("attributes.color")),
        clean_value(p.get("attributes.size")),
        clean_value(p.get("attributes.material")),

        clean_price(p.get("pricing.cost_price")),
        clean_price(p.get("pricing.selling_price")),
        p.get("pricing.currency"),

        p["supplier.supplier_id"],

        p.get("inventory.storage_type"),
        p.get("inventory.shelf_life_days"),

        p.get("logistics.weight_kg"),
        p.get("logistics.fragile", False),
        p.get("logistics.hazardous", False),

        p["status"],

        p["created_at"],
        p["updated_at"]

    )


def main():

    products = load_json()

    conn = get_connection()
    cur = conn.cursor()

    batch = []
    inserted = 0

    try:

        for product in tqdm(products, desc="Loading Products"):

            batch.append(build_row(product))

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
        print(f"Successfully inserted {inserted} products.")
        print("=" * 60)

    except Exception as e:

        conn.rollback()
        print("ERROR:", e)

    finally:

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

# import json
# from collections import Counter
#
# with open("../output/products.json", "r", encoding="utf-8") as f:
#     products = json.load(f)
#
# skus = [p["product_sku"] for p in products]
#
# duplicates = [sku for sku, c in Counter(skus).items() if c > 1]
#
# print("Duplicate SKUs:", len(duplicates))
#
# for sku in duplicates[:20]:
#     print(sku)