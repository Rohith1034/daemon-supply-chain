# Metadata-driven Spark ETL

## Run a pipeline

From the repository root, with the Spark and GCS connector dependencies and
PostgreSQL audit credentials configured:

```bash
PYTHONPATH="$(pwd)" python -m spark.jobs.run_pipeline purchase_order
```

The argument is a YAML filename under `spark/configs/` without `.yaml`.
For Airflow tasks, call `Pipeline(spark, event_name).run()`; keep discovery,
transformation, validation, writes, and audit work in the Spark package rather
than in the DAG.

The Airflow Compose services mount `spark/` read-only at `/opt/airflow/spark`,
which is importable through the configured `/opt/airflow` Python path. The
Silver DAGs create one Spark session per run, execute their configured
pipelines sequentially, and stop the session after success or failure.
Inbound and outbound simulation DAGs are scheduled every two hours at minute
00 (Asia/Kolkata); their Silver DAGs are staggered to minute 30. Silver tasks
retry twice after a five-minute delay. Set `AIRFLOW_ALERT_EMAIL` and configure
Airflow SMTP to enable task failure email notifications.

The session joins `spark://spark-master:7077` when launched under Airflow, or
when `ENV` is not `local`. Set `SPARK_MASTER_URL` to override the master. The
GCS connector coordinate and Application Default Credentials are configured
by `spark/framework/spark_session.py`.

## Current payload strategy

The repository does not contain event payload schemas under `spark/schemas/`.
The raw Bronze consumer envelope is standardized, so the configs use Spark
JSON schema inference and shared SQL templates to expand top-level fields of
the nested `payload` struct. Source envelope fields are preserved under
`_source_*` names, and the source GCS path is retained through transformation
for per-object audit counts, then removed before Silver Parquet is written.

This preserves unknown payload fields without inventing domain column names.
It is not a substitute for reviewed business mappings or an explicit output
schema. Add the event schemas under `spark/schemas/`, then configure
`source.schema.schema_location` and `validation.schema.schema_location` to
enable strict input and Silver-output schema validation. Nested payload
structs remain nested until their actual fields and required flattening rules
are specified in the corresponding Jinja SQL.

## PostgreSQL audit

The pipeline uses `silver_ingestion_audit` to claim source objects, skip
successful objects with an unchanged checksum, and record row counts and
success/failure state. Apply the existing
`../migrations/004_silver_ingestion_audit.sql` migration to the simulator
application database before running pipelines. Configure either
`SIMULATOR_DATABASE_URL` or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and
`DB_PASSWORD` in the Spark/Airflow environment.

## Pipeline configs

Each YAML config selects a Bronze topic, event types, Silver target, write
mode, keys, validation rules, and output options. `MERGE` currently uses
`_source_event_id` so each source event is preserved once; it is an event
ledger, not a domain-entity upsert. Replace that key and add domain SQL
mappings only after the actual entity schemas and update semantics are
available. Inbound configs process up to 100 files per batch and defer files
after the configured 500-file per-run limit to later runs.
