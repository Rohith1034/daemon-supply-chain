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

Apply `migrations/002_simulation_runs.sql` to PostgreSQL before starting the API. Each independent endpoint can then be called without a request body; PostgreSQL supplies the simulation ID, correlation ID, and default timestamp, and stores run status and results in `simulation_runs` instead of creating files under `output/simulation_runs`.

Each request runs one lifecycle cycle. A scheduler can call the inbound endpoint during the 00:00-12:00 window and the outbound endpoint during the 06:00-18:00 window, up to 120-150 times per window. The API serializes cycles against the shared simulator database; scheduling and cycle counts remain the caller's responsibility.

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