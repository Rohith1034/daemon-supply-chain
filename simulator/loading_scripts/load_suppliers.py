import json
from pathlib import Path
from psycopg2.extras import execute_values, Json
from tqdm import tqdm
from simulator.DB import get_connection

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_FILE = BASE_DIR / "output" / "suppliers.json"


BATCH_SIZE = 1000


INSERT_QUERY = """
INSERT INTO suppliers
(
    supplier_id,
    supplier_name,
    supplier_type,
    category_supported,
    contact_email,
    contact_phone,
    country,
    state,
    city,
    rating,
    payment_terms,
    lead_time_days,
    status,
    created_at,
    updated_at
)
VALUES %s
ON CONFLICT (supplier_id) DO NOTHING;
"""


def load_json():
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def build_row(s):

    contact = s.get("contact", {})
    address = s.get("address", {})

    return (

        s["supplier_id"],
        s["supplier_name"],
        s["supplier_type"],

        Json(s.get("category_supported", [])),

        contact.get("email"),
        contact.get("phone"),

        address.get("country"),
        address.get("state"),
        address.get("city"),

        s.get("rating"),
        s.get("payment_terms"),
        s.get("lead_time_days"),

        s["status"],

        s["created_at"],
        s["updated_at"]

    )


def main():

    suppliers = load_json()

    conn = get_connection()
    cur = conn.cursor()

    batch = []
    inserted = 0

    try:

        for supplier in tqdm(suppliers, desc="Loading Suppliers"):

            batch.append(build_row(supplier))

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

        print("=" * 50)
        print(f"Successfully inserted {inserted} suppliers.")
        print("=" * 50)

    except Exception as e:

        conn.rollback()
        print(e)

    finally:

        cur.close()
        conn.close()


if __name__ == "__main__":
    main()