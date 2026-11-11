import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from urllib.parse import unquote

from google.cloud import storage
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType

from spark.framework.metadata import load_metadata, resolve_spark_path


class Reader:
    def __init__(
        self,
        spark,
        event_name=None,
        audit=None,
        storage_client=None,
        metadata_config=None,
        sql=None,
    ):
        self.spark = spark
        self.event_name = event_name
        self.audit = audit
        self.storage_client = storage_client
        self.metadata_config = metadata_config
        self.sql = sql

    def _load_metadata(self):
        if self.metadata_config is not None and self.sql is not None:
            return self.metadata_config, self.sql
        if not self.event_name:
            raise ValueError("event_name is required to load pipeline metadata")
        self.metadata_config, self.sql = load_metadata(self.event_name)
        return self.metadata_config, self.sql

    def _storage(self):
        if self.storage_client is None:
            self.storage_client = storage.Client()
        return self.storage_client

    @staticmethod
    def _event_id(blob_name):
        filename = PurePosixPath(blob_name).name
        if filename.startswith("event_") and filename.endswith(".json"):
            return unquote(filename[len("event_") : -len(".json")])
        return None

    @staticmethod
    def _checksum(blob):
        metadata = blob.metadata or {}
        checksum = metadata.get("sha256")
        if (
            checksum
            and len(checksum) == 64
            and all(character in "0123456789abcdefABCDEF" for character in checksum)
        ):
            return checksum.lower()

        digest = hashlib.sha256()
        with blob.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _partition_prefixes(source):
        config = source.get("partition") or {}
        partition_type = config.get("type")
        if not partition_type or partition_type == "none":
            return [""]
        if partition_type != "hourly":
            raise ValueError(f"Unsupported source partition type: {partition_type}")

        lookback = config.get("lookback_hours", 1)
        if not isinstance(lookback, int) or lookback < 1:
            raise ValueError("source.partition.lookback_hours must be positive")
        now = datetime.now(timezone.utc)
        prefixes = []
        for offset in range(lookback):
            partition_time = now - timedelta(hours=offset)
            prefixes.append(
                f"{partition_time:%Y/%m/%d}/"
                f"hour={partition_time:%H}/"
            )
        return prefixes

    def discover_files(self, metadata_config=None):
        metadata_config = metadata_config or self._load_metadata()[0]
        source = metadata_config.get("source") or {}
        bucket_name = source.get("bucket_name")
        base_path = (source.get("base_path") or "").strip("/")
        event_types = source.get("event_types") or []
        if not bucket_name or not base_path or not event_types:
            raise ValueError(
                "source.bucket_name, source.base_path and source.event_types "
                "are required"
            )

        client = self._storage()
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            raise FileNotFoundError(f"GCS bucket not found: {bucket_name}")

        skip_processed = (metadata_config.get("checkpoint") or {}).get(
            "skip_processed_files", True
        )
        if skip_processed and self.audit is None:
            raise ValueError(
                "Audit service is required when skip_processed_files is enabled"
            )

        files = []
        prefixes = self._partition_prefixes(source)
        file_format = source.get("file_format", "json").lower()
        source_path_column = source.get(
            "source_path_column",
            "_source_gcs_path",
        )
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source_path_column):
            raise ValueError(
                "source.source_path_column must be a valid Spark column identifier"
            )
        extensions = {"json": ".json", "parquet": ".parquet"}
        if file_format not in extensions:
            raise ValueError(f"Unsupported Bronze source format: {file_format}")
        for event_type in event_types:
            for partition in prefixes:
                prefix = f"{base_path}/{event_type}/{partition}"
                for blob in client.list_blobs(bucket_name, prefix=prefix):
                    if not blob.name.endswith(extensions[file_format]):
                        continue
                    file_info = {
                        "gcs_path": f"gs://{bucket_name}/{blob.name}",
                        "event_id": self._event_id(blob.name),
                        "event_type": event_type,
                        "source_checksum": self._checksum(blob),
                    }
                    files.append(file_info)
        if not skip_processed:
            return files
        processed = self.audit.processed_paths(files)
        return [
            source_file
            for source_file in files
            if source_file["gcs_path"] not in processed
        ]

    def _schema(self, source):
        schema_config = source.get("schema") or {}
        schema_location = schema_config.get("schema_location")
        if schema_location:
            schema_path = resolve_spark_path(schema_location)
            if not schema_path.is_file():
                raise FileNotFoundError(f"Spark input schema not found: {schema_path}")
            with schema_path.open(encoding="utf-8") as file:
                schema_json = json.load(file)
            try:
                return StructType.fromJson(schema_json)
            except (TypeError, ValueError, KeyError) as exc:
                raise ValueError(f"Invalid Spark schema in {schema_path}: {exc}") from exc
        if schema_config.get("infer_schema", False):
            return None
        raise ValueError(
            "Configure source.schema.schema_location or enable "
            "source.schema.infer_schema"
        )

    def read_files(self, source_files, metadata_config=None):
        metadata_config = metadata_config or self._load_metadata()[0]
        if not source_files:
            raise FileNotFoundError("No unprocessed Bronze source files to read")

        source = metadata_config["source"]
        file_format = source.get("file_format", "json").lower()
        if file_format not in {"json", "parquet"}:
            raise ValueError(f"Unsupported Bronze source format: {file_format}")
        schema = self._schema(source) if file_format == "json" else None
        files_per_batch = (source.get("partition") or {}).get(
            "files_per_batch", 100
        )
        if not isinstance(files_per_batch, int) or files_per_batch < 1:
            raise ValueError("source.partition.files_per_batch must be positive")

        dataframes = []
        paths = [item["gcs_path"] for item in source_files]
        for start in range(0, len(paths), files_per_batch):
            batch = paths[start : start + files_per_batch]
            reader = self.spark.read
            if file_format == "json":
                reader = reader.option(
                    "multiLine",
                    bool(source.get("multi_line", False)),
                )
                if schema is not None:
                    reader = reader.schema(schema)
                dataframe = reader.json(batch)
            else:
                dataframe = reader.parquet(*batch)
            source_path_column = source.get(
                "source_path_column",
                "_source_gcs_path",
            )
            dataframe = dataframe.withColumn(
                source_path_column,
                input_file_name(),
            )
            dataframes.append(dataframe)

        dataframe = dataframes[0]
        for next_dataframe in dataframes[1:]:
            dataframe = dataframe.unionByName(
                next_dataframe,
                allowMissingColumns=True,
            )
        metadata_config, sql = self._load_metadata()
        return {
            "dataframe": dataframe,
            "metadata": metadata_config,
            "sql": sql,
            "source_files": source_files,
        }

    def reader(self):
        metadata_config, _ = self._load_metadata()
        return self.read_files(
            self.discover_files(metadata_config),
            metadata_config,
        )
