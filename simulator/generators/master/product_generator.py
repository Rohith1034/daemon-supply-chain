import pandas as pd
from faker import Faker
import random
import json
from datetime import datetime, timezone
from pathlib import Path

fake = Faker()

BASE_DIR = Path(__file__).resolve().parents[2]

df = pd.read_csv(BASE_DIR / "data" / "products.csv")
print(df.columns)

with open(BASE_DIR / "output" / "suppliers.json", "r") as f:
    suppliers = json.load(f)

suppliers_list = [
    {
        "supplier_id": supplier["supplier_id"],
        "category": supplier["category_supported"][0]
    }
    for supplier in suppliers
]


def random_supplier_id(category):
    matching_suppliers = [supplier["supplier_id"] for supplier in suppliers_list if supplier["category"] == category]
    if not matching_suppliers:
        raise ValueError(f"No supplier found for category: {category}")
    return random.choice(matching_suppliers)


print(random_supplier_id("Beauty & Personal Care"))

products = []

for i, row in df.iterrows():
    product = {
        "product_id": f"PROD-{i + 1:08d}",
        "product_sku": f"{row['brand'][:3].upper()}-{random.randint(10000, 99999)}",
        "product_barcode": fake.ean13(),
        "name": row["name"],
        "description": row["description"],
        "category.level_1": row["category.level_1"],
        "category.level_2": row["category.level_2"],
        "category.level_3": row["category.level_3"],
        "brand": row["brand"],
        "attributes.color": row["attributes.color"],
        "attributes.size": row["attributes.size"],
        "attributes.material": row["attributes.material"],
        "pricing.cost_price": row["pricing.cost_price"],
        "pricing.selling_price": row["pricing.selling_price"],
        "pricing.currency": row["pricing.currency"],
        "supplier.supplier_id": random_supplier_id(row["category.level_1"]),
        "inventory.storage_type": row["inventory.storage_type"],
        "inventory.shelf_life_days": row["inventory.shelf_life_days"],
        "logistics.weight_kg": row["logistics.weight_kg"],
        "logistics.fragile": row["logistics.fragile"],
        "logistics.hazardous": row["logistics.hazardous"],
        "status": random.choice(
            [
                "ACTIVE",
                "ACTIVE",
                "ACTIVE",
                "ACTIVE",
                "ACTIVE",
                "INACTIVE"
            ]
        ),

        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "updated_at": datetime.now(
            timezone.utc
        ).isoformat()

    }
    products.append(product)


with open(
        BASE_DIR / "output" / "products.json",
    "w"
) as f:

    json.dump(
        products,
        f,
        indent=4
    )


print(
    f"Generated {len(products)} products"
)