import time

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number


class Writer:
    def __init__(self, dataframe, metadata, spark):
        self.dataframe = dataframe
        self.metadata = metadata
        self.spark = spark

    def writer(self):
        started = time.monotonic()
        if not self.metadata:
            raise ValueError("Missing metadata")
        if self.dataframe is None:
            raise ValueError("Missing dataframe")

        target = self.metadata.get("target") or {}
        if not target:
            raise ValueError("Missing target metadata")

        bucket_name = target.get("bucket_name")
        folder_name = target.get("folder_name")
        file_format = target.get("file_format", "parquet").lower()
        if not bucket_name or not folder_name:
            raise ValueError("Target bucket_name and folder_name are required")
        if file_format != "parquet":
            raise ValueError(f"Unsupported Silver file format: {file_format}")

        processing = self.metadata.get("processing") or {}
        write_mode = processing.get("write_mode", "APPEND").upper()
        primary_keys = processing.get("primary_keys") or []
        if write_mode not in {"APPEND", "OVERWRITE", "MERGE"}:
            raise ValueError(f"Unsupported write_mode: {write_mode}")
        if write_mode == "MERGE" and not primary_keys:
            raise ValueError("MERGE requires processing.primary_keys")

        output = self.metadata.get("output") or {}
        compression = output.get("compression", "snappy")
        partition_by = output.get(
            "partition_by",
            target.get("partition_columns", []),
        ) or []
        if not isinstance(partition_by, list):
            raise ValueError("output.partition_by must be a list")
        missing_partitions = set(partition_by) - set(self.dataframe.columns)
        if missing_partitions:
            raise ValueError(
                f"Partition columns missing from dataframe: "
                f"{sorted(missing_partitions)}"
            )

        silver_path = f"gs://{bucket_name.rstrip('/')}/{folder_name.strip('/')}"
        exists = (
            self._path_exists(silver_path)
            if write_mode == "MERGE"
            else False
        )
        merge_dataframe = None

        if write_mode == "MERGE":
            merge_dataframe = self._merge_with_existing(
                silver_path,
                exists,
                primary_keys,
            )
            dataframe = merge_dataframe
            effective_save_mode = "overwrite"
        else:
            dataframe = self.dataframe
            effective_save_mode = {
                "APPEND": "append",
                "OVERWRITE": "overwrite",
            }[write_mode]

        source_path_column = (self.metadata.get("source") or {}).get(
            "source_path_column",
            "_source_gcs_path",
        )
        if source_path_column in dataframe.columns:
            dataframe = dataframe.drop(source_path_column)

        writer = (
            dataframe.write
            .format("parquet")
            .mode(effective_save_mode)
            .option("compression", compression)
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        try:
            if merge_dataframe is not None:
                merge_dataframe.count()
            rows_written = self.dataframe.count()
            writer.save(silver_path)
        finally:
            if merge_dataframe is not None:
                merge_dataframe.unpersist()

        return {
            "status": "SUCCESS",
            "rows_written": rows_written,
            "target_path": silver_path,
            "execution_time": time.monotonic() - started,
        }

    def _path_exists(self, path):
        jvm = self.spark.sparkContext._jvm
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        target_path = jvm.org.apache.hadoop.fs.Path(path)
        return target_path.getFileSystem(hadoop_conf).exists(target_path)

    def _merge_with_existing(self, silver_path, exists, primary_keys):
        missing_keys = set(primary_keys) - set(self.dataframe.columns)
        if missing_keys:
            raise ValueError(
                f"Primary key columns missing from dataframe: "
                f"{sorted(missing_keys)}"
            )
        self._validate_unique_keys(self.dataframe, primary_keys, "incoming")

        if exists:
            existing = self.spark.read.parquet(silver_path)
            missing_existing_keys = set(primary_keys) - set(existing.columns)
            if missing_existing_keys:
                raise ValueError(
                    f"Primary key columns missing from existing Silver data: "
                    f"{sorted(missing_existing_keys)}"
                )
            self._validate_unique_keys(existing, primary_keys, "existing Silver")
        else:
            existing = self.dataframe.limit(0)

        marker = "__silver_merge_incoming"
        while marker in self.dataframe.columns or marker in existing.columns:
            marker = f"_{marker}"
        rank_column = "__silver_merge_rank"
        while (
            rank_column in self.dataframe.columns
            or rank_column in existing.columns
            or rank_column == marker
        ):
            rank_column = f"_{rank_column}"

        combined = existing.withColumn(marker, lit(False)).unionByName(
            self.dataframe.withColumn(
                marker,
                lit(True),
            ),
            allowMissingColumns=True,
        )
        window = Window.partitionBy(*primary_keys).orderBy(col(marker).desc())
        merged = (
            combined
            .withColumn(rank_column, row_number().over(window))
            .filter(col(rank_column) == 1)
            .drop(marker, rank_column)
        )
        return merged.persist(StorageLevel.MEMORY_AND_DISK)

    @staticmethod
    def _validate_unique_keys(dataframe, primary_keys, description):
        missing = set(primary_keys) - set(dataframe.columns)
        if missing:
            raise ValueError(
                f"Primary key columns missing from {description} data: "
                f"{sorted(missing)}"
            )
        null_condition = col(primary_keys[0]).isNull()
        for key in primary_keys[1:]:
            null_condition = null_condition | col(key).isNull()
        null_keys = dataframe.filter(null_condition)
        if null_keys.limit(1).count():
            raise ValueError(f"{description} data contains null primary keys")
        duplicates = (
            dataframe.groupBy(*primary_keys)
            .count()
            .filter(col("count") > 1)
        )
        if duplicates.limit(1).count():
            raise ValueError(f"{description} data contains duplicate primary keys")
