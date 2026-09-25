import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.db import Database
from generators.warehouse.goods_received import generate_goods_received
from generators.warehouse.packing_completed import generate_packing_completed


def test_packing_completed_accepts_packing_row_in_pack_state():
    packing_task_id = f"PACK-{uuid.uuid4().hex[:8].upper()}"
    picking_task_id = f"PICK-{uuid.uuid4().hex[:8].upper()}"
    order_id = f"ORDER-{uuid.uuid4().hex[:8].upper()}"
    worker_id = f"WK-{uuid.uuid4().hex[:8].upper()}"
    now = datetime.now(timezone.utc)

    with Database() as db:
        db.execute(
            """
            INSERT INTO customers (
                customer_id, first_name, last_name, email, phone, country,
                state, city, customer_segment, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            (
                "CUST-1",
                "Test",
                "Customer",
                "test.customer@example.com",
                "555-0100",
                "USA",
                "CA",
                "San Francisco",
                "B2B",
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO warehouses (
                warehouse_id, warehouse_code, warehouse_name, warehouse_type,
                status, country, state, state_code, city, postal_code,
                timezone, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (warehouse_id) DO NOTHING
            """,
            (
                "WH-00001",
                "WH01",
                "Test Warehouse One",
                "DISTRIBUTION",
                "ACTIVE",
                "USA",
                "CA",
                "CA",
                "San Francisco",
                "94105",
                "UTC",
                now,
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO warehouse_locations (
                location_id, warehouse_id, zone, aisle, rack, shelf, bin,
                storage_type, capacity_units, current_utilization,
                temperature_controlled, hazmat_allowed, status, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id) DO NOTHING
            """,
            (
                "A1",
                "WH-00001",
                "A",
                "1",
                "R1",
                "S1",
                "B1",
                "PICKABLE",
                100,
                0,
                False,
                False,
                "ACTIVE",
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO suppliers (
                supplier_id, supplier_name, supplier_type, category_supported,
                contact_email, contact_phone, country, state, city,
                rating, payment_terms, lead_time_days, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (supplier_id) DO NOTHING
            """,
            (
                "SUP-100",
                "Test Supplier",
                "MANUFACTURER",
                "FOOD",
                "supplier@example.com",
                "555-0200",
                "USA",
                "TX",
                "Dallas",
                4.8,
                "NET30",
                7,
                "ACTIVE",
                now,
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO products (
                product_id, sku, barcode, name, description, supplier_id,
                fragile, hazardous, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            """,
            (
                "PROD-TEST-1",
                "SKU-TEST-1",
                "BAR-TEST-1",
                "Test Product",
                "Test product for warehouse task state contract",
                "SUP-100",
                False,
                False,
                "ACTIVE",
                now,
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO workers (
                worker_id, employee_code, first_name, last_name, role,
                skill_set, warehouse_id, shift_type, employment_status,
                hire_date, created_at, experience_years, productivity_rating,
                certifications, current_status, shift_start_time, shift_end_time,
                hourly_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                worker_id,
                "EMP-100",
                "Packing",
                "Worker",
                "PACKER",
                json.dumps(["PACKING"]),
                "WH-00001",
                "DAY",
                "ACTIVE",
                now.date(),
                now,
                2,
                100,
                json.dumps(["CERT-1"]),
                "AVAILABLE",
                now,
                now,
                22.5,
            ),
        )
        db.execute(
            """
            INSERT INTO orders (
                order_id, customer_id, warehouse_id, order_status, order_channel,
                priority, total_items, total_quantity, total_amount, currency,
                order_date, created_at, confirmed_at, promised_delivery_date,
                correlation_id, items_created
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                "CUST-1",
                "WH-00001",
                "PACKING",
                "WEB",
                "HIGH",
                1,
                5,
                50.0,
                "USD",
                now.date(),
                now,
                now,
                now + timedelta(days=3),
                str(uuid.uuid4()),
                True,
            ),
        )
        db.execute(
            """
            INSERT INTO warehouse_tasks (
                task_id, task_type, warehouse_id, product_id, location,
                quantity, priority, status, assigned_worker_id,
                estimated_minutes, actual_minutes, created_at,
                task_started_at, task_completed_at, created_by,
                assigned_at, started_by, completed_by, correlation_id,
                expected_quantity, order_id, picking_task_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                picking_task_id,
                "PICKING",
                "WH-00001",
                "PROD-TEST-1",
                "A1",
                5,
                "NORMAL",
                "COMPLETED",
                worker_id,
                15,
                10,
                now,
                now,
                now,
                "SYSTEM",
                now,
                "SYSTEM",
                "SYSTEM",
                str(uuid.uuid4()),
                5,
                order_id,
                None,
            ),
        )
        db.execute(
            """
            INSERT INTO warehouse_tasks (
                task_id, task_type, warehouse_id, product_id, location,
                quantity, priority, status, assigned_worker_id,
                estimated_minutes, actual_minutes, created_at,
                task_started_at, task_completed_at, created_by,
                assigned_at, started_by, completed_by, correlation_id,
                expected_quantity, order_id, picking_task_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                packing_task_id,
                "PACKING",
                "WH-00001",
                "PROD-TEST-1",
                "A1",
                5,
                "NORMAL",
                "PACKING",
                worker_id,
                15,
                0,
                now,
                now,
                None,
                "SYSTEM",
                now,
                "SYSTEM",
                None,
                str(uuid.uuid4()),
                5,
                order_id,
                picking_task_id,
            ),
        )

        result = generate_packing_completed(task_id=packing_task_id)

        db.execute("SELECT status FROM warehouse_tasks WHERE task_id=%s", (packing_task_id,))
        row = db.cursor.fetchone()
        assert row["status"] == "COMPLETED"
        assert result["task_id"] == packing_task_id



def test_goods_received_accepts_started_receiving_task():
    task_id = f"RECV-{uuid.uuid4().hex[:8].upper()}"
    shipment_id = f"SHIP-{uuid.uuid4().hex[:8].upper()}"
    worker_id = f"WK-{uuid.uuid4().hex[:8].upper()}"
    now = datetime.now(timezone.utc)

    with Database() as db:
        db.execute(
            """
            INSERT INTO suppliers (
                supplier_id, supplier_name, supplier_type, category_supported,
                contact_email, contact_phone, country, state, city,
                rating, payment_terms, lead_time_days, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (supplier_id) DO NOTHING
            """,
            (
                "SUP-100",
                "Test Supplier",
                "MANUFACTURER",
                "FOOD",
                "supplier@example.com",
                "555-0200",
                "USA",
                "TX",
                "Dallas",
                4.8,
                "NET30",
                7,
                "ACTIVE",
                now,
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO warehouses (
                warehouse_id, warehouse_code, warehouse_name, warehouse_type,
                status, country, state, state_code, city, postal_code,
                timezone, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (warehouse_id) DO NOTHING
            """,
            (
                "WH-00002",
                "WH02",
                "Test Warehouse Two",
                "DISTRIBUTION",
                "ACTIVE",
                "USA",
                "TX",
                "TX",
                "Dallas",
                "75201",
                "UTC",
                now,
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO products (
                product_id, sku, barcode, name, description, supplier_id,
                fragile, hazardous, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            """,
            (
                "PROD-RECV-1",
                "SKU-RECV-1",
                "BAR-RECV-1",
                "Receiving Test Product",
                "Test product for receiving flow",
                "SUP-100",
                False,
                False,
                "ACTIVE",
                now,
                now,
            ),
        )
        db.execute(
            """
            INSERT INTO workers (
                worker_id, employee_code, first_name, last_name, role,
                skill_set, warehouse_id, shift_type, employment_status,
                hire_date, created_at, experience_years, productivity_rating,
                certifications, current_status, shift_start_time, shift_end_time,
                hourly_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                worker_id,
                "EMP-101",
                "Receiving",
                "Worker",
                "RECEIVER",
                json.dumps(["RECEIVING"]),
                "WH-00002",
                "DAY",
                "ACTIVE",
                now.date(),
                now,
                3,
                95,
                json.dumps(["CERT-2"]),
                "AVAILABLE",
                now,
                now,
                18.5,
            ),
        )
        db.execute(
            """
            INSERT INTO shipments (
                shipment_id, po_id, supplier_id, warehouse_id, shipment_status,
                shipment_date, expected_delivery, actual_delivery, total_skus,
                total_quantity, created_at, updated_at, correlation_id,
                receiving_task_created, carrier_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                shipment_id,
                "PO-STATE-TEST",
                "SUP-100",
                "WH-00002",
                "RECEIVING",
                now.date(),
                now + timedelta(days=2),
                None,
                1,
                10,
                now,
                now,
                str(uuid.uuid4()),
                True,
                None,
            ),
        )
        db.execute(
            """
            INSERT INTO shipment_items (
                shipment_id, product_id, shipped_quantity, received_quantity, damaged_quantity
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (shipment_id, "PROD-RECV-1", 10, 0, 0),
        )
        db.execute(
            """
            INSERT INTO warehouse_tasks (
                task_id, task_type, warehouse_id, shipment_id, product_id,
                quantity, priority, status, assigned_worker_id,
                estimated_minutes, actual_minutes, created_at,
                task_started_at, task_completed_at, created_by,
                assigned_at, started_by, completed_by, correlation_id,
                dock_location, expected_quantity, received_quantity, order_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                task_id,
                "RECEIVING",
                "WH-00002",
                shipment_id,
                "PROD-RECV-1",
                10,
                "HIGH",
                "STARTED",
                worker_id,
                30,
                0,
                now,
                now,
                None,
                "SYSTEM",
                now,
                "SYSTEM",
                None,
                str(uuid.uuid4()),
                "DOCK-001",
                10,
                0,
                None,
            ),
        )

        result = generate_goods_received(task_id=task_id)

        db.execute("SELECT status FROM warehouse_tasks WHERE task_id=%s", (task_id,))
        row = db.cursor.fetchone()
        assert row["status"] == "COMPLETED"
        assert result["task_id"] == task_id

