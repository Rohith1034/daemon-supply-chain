# Demon Supply Chain Platform

End-to-end supply chain data engineering and AI/ML platform.

## Architecture

Simulator
↓
PostgreSQL
↓
Outbox Pattern
↓
Kafka
↓
GCS Bronze
↓
Spark
↓
BigQuery
↓
ML Models


## Services

- FastAPI
- PostgreSQL
- Kafka
- Airflow
- Spark
- GCP Storage
- BigQuery


## Domains

- Supplier
- Procurement
- Inventory
- Warehouse
- Orders
- Transportation
- Delivery

## Independent Simulator Runs

Run each lifecycle independently from the repository root:

```powershell
.\.venv\Scripts\python.exe simulator\loading_scripts\run_inbound_flow.py
.\.venv\Scripts\python.exe simulator\loading_scripts\run_outbound_flow.py
.\.venv\Scripts\python.exe simulator\loading_scripts\run_transportation_flow.py
```

To reset transactional tables and run all three flows three times, use:

```powershell
.\.venv\Scripts\python.exe scripts\run_independent_flow_validation.py 3
```

The validation runner clears only the reviewed transactional-table allowlist before each flow and verifies that required master-table counts remain unchanged. It preserves `product_supplier_mapping` as reference data.