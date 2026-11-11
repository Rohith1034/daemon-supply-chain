import json
import os
import sys

import pytest
from pyspark.sql import Row, SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from spark.framework.metadata import load_metadata
from spark.framework.audit import Audit
from spark.framework import pipeline as pipeline_module
from spark.framework.reader import Reader
from spark.framework.transformation import Transformation
from spark.framework.validator import Validator
from spark.framework.writer import Writer


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("spark-framework-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_metadata_loader_renders_configured_event_types():
    metadata, sql = load_metadata("purchase_order")

    assert metadata["source"]["event_types"]
    for event_type in metadata["source"]["event_types"]:
        assert f"'{event_type}'" in sql


def test_all_event_metadata_uses_configured_types_and_reusable_sql():
    pipelines = {
        "purchase_order": {"PurchaseOrderCreated", "PurchaseOrderApproved"},
        "supplier_shipment": {
            "SupplierShipmentCreated",
            "SupplierShipmentDispatched",
            "SupplierShipmentDelivered",
        },
        "goods_receipt": {"GoodsReceived"},
        "inventory": {
            "InventoryUpdated",
            "InventoryPutaway",
        },
        "order": {"OrderCreated", "OrderItemCreated"},
        "inventory_reservation": {"InventoryReserved"},
        "warehouse_task": {"Picked", "Packed", "WarehouseTaskCompleted"},
        "shipment": {
            "ShipmentCreated",
            "ShipmentDispatched",
            "ShipmentDelivered",
        },
        "purchase_order_items": {
            "PurchaseOrderCreated",
            "PurchaseOrderApproved",
        },
    }

    for pipeline, expected_events in pipelines.items():
        metadata, sql = load_metadata(pipeline)
        assert set(metadata["source"]["event_types"]) == expected_events
        assert "_source_gcs_path" in sql
        if pipeline == "purchase_order_items":
            assert metadata["source"]["file_format"] == "parquet"
        else:
            assert metadata["source"]["schema"]["infer_schema"] is True
            assert "payload.*" in sql


def test_transformation_runs_metadata_sql(spark):
    source = spark.createDataFrame([(1, "raw")], ["id", "status"])
    metadata = {"processing": {"source_view": "source_for_test"}}

    transformed = Transformation(
        dataframe=source,
        metadata=metadata,
        sql="SELECT id, upper(status) AS status FROM source_for_test",
        spark=spark,
    ).transform()

    assert transformed.collect() == [(1, "RAW")]


def test_event_sql_flattens_inferred_payload_struct(spark):
    source = spark.createDataFrame(
        [
            (
                "event-1",
                "PurchaseOrderCreated",
                "2026-11-12T00:00:00Z",
                "purchase-order-1",
                "gs://bronze/raw/event_1.json",
                ("PO-1", "CREATED"),
            )
        ],
        [
            "event_id",
            "event_type",
            "created_at",
            "aggregate_id",
            "_source_gcs_path",
            "payload",
        ],
    )
    source = source.selectExpr(
        "event_id",
        "event_type",
        "created_at",
        "aggregate_id",
        "_source_gcs_path",
        "named_struct('po_id', payload._1, 'status', payload._2) AS payload",
    )
    metadata, sql = load_metadata("purchase_order")
    transformed = Transformation(source, metadata, sql, spark).transform()

    row = transformed.first()
    assert row["_source_event_id"] == "event-1"
    assert row["po_id"] == "PO-1"
    assert row["status"] == "CREATED"
    assert row["_source_gcs_path"] == "gs://bronze/raw/event_1.json"


def test_generic_sql_executes_for_each_event_pipeline(spark):
    pipeline_names = (
        "purchase_order",
        "supplier_shipment",
        "goods_receipt",
        "inventory",
        "inventory_reservation",
        "order",
        "warehouse_task",
        "shipment",
    )
    for pipeline_name in pipeline_names:
        metadata, sql = load_metadata(pipeline_name)
        event_type = metadata["source"]["event_types"][0]
        source = spark.createDataFrame(
            [
                (
                    "event-1",
                    event_type,
                    "2026-11-12T00:00:00Z",
                    "aggregate-1",
                    "gs://bronze/raw/event_1.json",
                    Row(business_value="value"),
                )
            ],
            [
                "event_id",
                "event_type",
                "created_at",
                "aggregate_id",
                "_source_gcs_path",
                "payload",
            ],
        )
        result = Transformation(source, metadata, sql, spark).transform().first()

        assert result["_source_event_type"] == event_type
        assert result["business_value"] == "value"


def test_reader_infers_json_and_adds_source_file_lineage(spark, tmp_path):
    source_path = tmp_path / "event.json"
    source_path.write_text(
        '{"event_id":"event-1","event_type":"Example","payload":{"value":7}}',
        encoding="utf-8",
    )
    source_uri = source_path.as_uri()
    metadata = {
        "source": {
            "file_format": "json",
            "schema": {"infer_schema": True},
            "source_path_column": "_source_gcs_path",
            "partition": {"files_per_batch": 1},
        }
    }

    dataframe = Reader(
        spark,
        metadata_config=metadata,
        sql="SELECT * FROM source_dataframe",
    ).read_files(
        [
            {
                "gcs_path": source_uri,
                "event_id": "event-1",
                "event_type": "Example",
                "source_checksum": "0" * 64,
            }
        ],
        metadata,
    )["dataframe"]
    row = dataframe.first()

    assert row.event_id == "event-1"
    assert row.payload.value == 7
    assert row["_source_gcs_path"] == source_uri


def test_validator_returns_structured_metadata_driven_failures(spark):
    dataframe = spark.createDataFrame(
        [(1, "present"), (1, None)],
        ["id", "description"],
    )
    result = Validator(
        dataframe,
        {
            "validation": {
                "required_columns": ["id", "missing"],
                "not_null_columns": ["description"],
                "unique_columns": ["id"],
            },
            "processing": {"primary_keys": ["id"]},
        },
    ).validate()

    assert result["success"] is False
    assert result["metrics"]["rows_checked"] == 2
    assert "missing" in result["errors"][0]
    assert result["metrics"]["null_failures"]["description"] == 1
    assert result["metrics"]["duplicate_failures"]["id"] == 1


def test_schema_validation_checks_declared_field_types(spark, tmp_path):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "id",
                        "type": "long",
                        "nullable": False,
                        "metadata": {},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    dataframe = spark.createDataFrame([("1",)], ["id"])

    result = Validator(
        dataframe,
        {
            "validation": {
                "schema": {
                    "enabled": True,
                    "schema_location": str(schema_path),
                    "allow_extra_columns": False,
                }
            },
        },
    ).validate()

    assert result["success"] is False
    assert result["metrics"]["schema_failures"][0]["type"] == "type_mismatches"


def test_writer_uses_append_and_returns_write_metrics():
    class FakeWriter:
        def __init__(self):
            self.options = {}

        def format(self, value):
            self.options["format"] = value
            return self

        def mode(self, value):
            self.options["mode"] = value
            return self

        def option(self, key, value):
            self.options[key] = value
            return self

        def partitionBy(self, *columns):
            self.options["partition_by"] = columns
            return self

        def save(self, path):
            self.options["path"] = path

    class FakeDataFrame:
        columns = ["id"]

        def __init__(self):
            self.write = FakeWriter()

        @staticmethod
        def count():
            return 3

    dataframe = FakeDataFrame()
    writer = Writer(
        dataframe,
        {
            "target": {
                "bucket_name": "silver-bucket",
                "folder_name": "entities",
                "file_format": "parquet",
            },
            "processing": {"write_mode": "APPEND"},
            "output": {"compression": "snappy"},
        },
        spark=None,
    )
    writer._path_exists = lambda _path: False

    result = writer.writer()

    assert dataframe.write.options["mode"] == "append"
    assert dataframe.write.options["format"] == "parquet"
    assert result["status"] == "SUCCESS"
    assert result["rows_written"] == 3
    assert result["target_path"] == "gs://silver-bucket/entities"


def test_audit_batches_processed_lookups_and_completion_updates():
    class FakeCursor:
        def __init__(self, rows):
            self.rows = rows
            self.query = None
            self.parameters = None

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, query, parameters):
            self.query = query
            self.parameters = parameters

        def fetchall(self):
            return self.rows

    class FakeConnection:
        def __init__(self, rows):
            self.cursor_instance = FakeCursor(rows)
            self.committed = False
            self.closed = False

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            self.committed = True

        def rollback(self):
            raise AssertionError("Unexpected audit rollback")

        def close(self):
            self.closed = True

    connections = iter(
        [
            FakeConnection([("gs://bronze/raw/event_1.json",)]),
            FakeConnection([(7,)]),
        ]
    )
    audit = Audit(connection_factory=lambda: next(connections))
    source_files = [
        {
            "gcs_path": "gs://bronze/raw/event_1.json",
            "event_id": "event-1",
            "source_checksum": "a" * 64,
        }
    ]

    assert audit.processed_paths(source_files) == {
        "gs://bronze/raw/event_1.json"
    }
    audit.complete_files(
        [{"ingestion_id": 7, "rows_read": 1, "rows_written": 1}]
    )


def test_merge_prefers_incoming_rows_and_preserves_existing_rows(spark, tmp_path):
    existing_path = str(tmp_path / "existing")
    spark.createDataFrame(
        [("PO001", "CREATED"), ("PO002", "CREATED")],
        ["po_id", "status"],
    ).write.parquet(existing_path)
    incoming = spark.createDataFrame(
        [("PO001", "APPROVED"), ("PO003", "CREATED")],
        ["po_id", "status"],
    )

    merged = Writer(incoming, {}, spark)._merge_with_existing(
        existing_path,
        True,
        ["po_id"],
    )
    try:
        actual = {row.po_id: row.status for row in merged.collect()}
    finally:
        merged.unpersist()

    assert actual == {
        "PO001": "APPROVED",
        "PO002": "CREATED",
        "PO003": "CREATED",
    }


def test_pipeline_processes_bounded_file_batches(spark, monkeypatch):
    source_files = [
        {
            "gcs_path": f"gs://bronze/raw/event_{index}.json",
            "event_id": f"event-{index}",
        }
        for index in range(4)
    ]
    read_batches = []
    metadata = {
        "source": {
            "source_path_column": "_source_gcs_path",
            "partition": {"files_per_batch": 2, "max_files_per_run": 3},
        },
        "target": {"folder_name": "entities"},
        "audit": {"enabled": False},
        "checkpoint": {"skip_processed_files": False},
    }

    class FakeReader:
        def __init__(self, *_args, **_kwargs):
            pass

        @staticmethod
        def discover_files(_metadata):
            return source_files

        def read_files(self, files, _metadata):
            paths = [item["gcs_path"] for item in files]
            read_batches.append(paths)
            return {
                "dataframe": spark.createDataFrame(
                    [(path,) for path in paths],
                    ["_source_gcs_path"],
                ),
                "sql": "SELECT _source_gcs_path",
            }

    class FakeTransformation:
        def __init__(self, dataframe, *_args, **_kwargs):
            self.dataframe = dataframe

        def transform(self):
            return self.dataframe

    class FakeValidator:
        def __init__(self, *_args, **_kwargs):
            pass

        @staticmethod
        def validate():
            return {"success": True, "errors": []}

    class FakeWriter:
        def __init__(self, dataframe, *_args, **_kwargs):
            self.dataframe = dataframe

        def writer(self):
            return {"rows_written": self.dataframe.count()}

    monkeypatch.setattr(
        pipeline_module,
        "load_metadata",
        lambda _name: (metadata, "SELECT _source_gcs_path"),
    )
    monkeypatch.setattr(pipeline_module, "Reader", FakeReader)
    monkeypatch.setattr(pipeline_module, "Transformation", FakeTransformation)
    monkeypatch.setattr(pipeline_module, "Validator", FakeValidator)
    monkeypatch.setattr(pipeline_module, "Writer", FakeWriter)

    result = pipeline_module.Pipeline(spark, "test_pipeline").run()

    assert read_batches == [
        ["gs://bronze/raw/event_0.json", "gs://bronze/raw/event_1.json"],
        ["gs://bronze/raw/event_2.json"],
    ]
    assert result["files_processed"] == 3
    assert result["files_deferred"] == 1
    assert result["rows_written"] == 3
