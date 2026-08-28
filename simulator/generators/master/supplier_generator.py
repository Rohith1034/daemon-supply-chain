from faker import Faker
import random
import json
from datetime import datetime, timezone
from pathlib import Path

fake = Faker()

# categories = [
#     "Beauty & Personal Care",
#     "Pharmacy",
#     "Essentials",
#     "DIY & Hardware",
#     "Furniture",
#     "Toys & Games",
#     "Grocery & Essentials",
#     "Electronics"
# ]
#
#
# supplier_prefix = {
#     "Beauty & Personal Care": [
#         "Beauty", "Cosmetics", "Glow", "Care", "Nature"
#     ],
#     "Pharmacy": [
#         "Health", "Med", "Life", "Wellness", "Bio"
#     ],
#     "Essentials": [
#         "Daily", "Home", "Essential", "Comfort"
#     ],
#     "DIY & Hardware": [
#         "Build", "Pro", "Tool", "Hardware"
#     ],
#     "Furniture": [
#         "Wood", "Home", "Living", "Furniture"
#     ],
#     "Toys & Games": [
#         "Play", "Fun", "Kids", "Toy"
#     ],
#     "Grocery & Essentials": [
#         "Fresh", "Farm", "Food", "Organic"
#     ],
#     "Electronics": [
#         "Tech", "Digital", "Smart", "Electro"
#     ]
# }
#
#
# supplier_types = [
#     "MANUFACTURER",
#     "WHOLESALER",
#     "DISTRIBUTOR",
#     "IMPORTER"
# ]
#
#
# payment_terms = [
#     "NET_15",
#     "NET_30",
#     "NET_45",
#     "NET_60"
# ]
#
#
# suppliers = []
#
#
# for i in range(1,401):
#
#     category = random.choice(categories)
#
#     prefix = random.choice(
#         supplier_prefix[category]
#     )
#
#     supplier_name = (
#         f"{prefix} "
#         f"{fake.company_suffix()}"
#     )
#
#
#     supplier = {
#
#         "supplier_id": f"SUP-{i:05d}",
#
#         "supplier_name": supplier_name,
#
#         "supplier_type": random.choice(
#             supplier_types
#         ),
#
#         "category_supported": [
#             category
#         ],
#
#         "contact": {
#
#             "email": fake.company_email(),
#
#             "phone": fake.phone_number()
#
#         },
#
#
#         "address": {
#
#             "country": fake.country(),
#
#             "state": fake.state(),
#
#             "city": fake.city()
#
#         },
#
#
#         "rating": round(
#             random.uniform(3.5,5.0),
#             1
#         ),
#
#
#         "payment_terms": random.choice(
#             payment_terms
#         ),
#
#
#         "lead_time_days": random.randint(
#             3,
#             45
#         ),
#
#
#         "status": random.choice(
#             [
#                 "ACTIVE",
#                 "ACTIVE",
#                 "ACTIVE",
#                 "INACTIVE"
#             ]
#         ),
#
#
#         "created_at": datetime.now(
#             timezone.utc
#         ).isoformat(),
#
#         "updated_at": datetime.now(
#             timezone.utc
#         ).isoformat()
#
#     }
#
#
#     suppliers.append(supplier)



BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_FILE = BASE_DIR / "output" / "suppliers.json"

categories = [
    "Beauty & Personal Care",
    "Pharmacy",
    "Essentials",
    "DIY & Hardware",
    "Furniture",
    "Toys & Games",
    "Grocery & Essentials",
    "Electronics"
]

suppliers = []

for index in range(1, 401):
    now = datetime.now(timezone.utc).isoformat()
    category = random.choice(categories)

    suppliers.append(
        {
            "supplier_id": f"SUP-{index:05d}",
            "supplier_name": fake.company(),
            "supplier_type": random.choice(
                ["MANUFACTURER", "WHOLESALER", "DISTRIBUTOR", "IMPORTER"]
            ),
            "category_supported": [category],
            "contact": {
                "email": fake.company_email(),
                "phone": fake.phone_number()
            },
            "address": {
                "country": fake.country(),
                "state": fake.state(),
                "city": fake.city()
            },
            "rating": round(random.uniform(3.5, 5.0), 1),
            "payment_terms": random.choice(
                ["NET_15", "NET_30", "NET_45", "NET_60"]
            ),
            "lead_time_days": random.randint(3, 45),
            "status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]),
            "created_at": now,
            "updated_at": now
        }
    )

with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
    json.dump(suppliers, file, indent=4)

print(f"Generated {len(suppliers)} suppliers")