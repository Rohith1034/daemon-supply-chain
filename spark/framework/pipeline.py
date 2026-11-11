import logging

from pyspark.sql.functions import col

from spark.framework.audit import Audit
from spark.framework.metadata import load_metadata
from spark.framework.reader import Reader
from spark.framework.transformation import Transformation
from spark.framework.validator import Validator
from spark.framework.writer import Writer


LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, spark, event_name, audit=None):
        if not spark:
            raise ValueError("Pipeline requires a SparkSession")
        if not event_name:
            raise ValueError("Pipeline requires a metadata config name")
        self.spark = spark
        self.event_name = event_name
        self.audit = audit

    def run(self):
        metadata, sql = load_metadata(self.event_name)
        job = metadata.get("job") or {}
        if job.get("enabled", True) is False:
            return {
                "status": "SKIPPED",
                "pipeline": self.event_name,
                "reason": "Pipeline is disabled in metadata",
            }

        audit_config = metadata.get("audit") or {}
        checkpoint_config = metadata.get("checkpoint") or {}
        audit_required = audit_config.get("enabled", True) or checkpoint_config.get(
            "skip_processed_files", True
        )
        audit = self.audit or (
            Audit(
                stale_after_minutes=audit_config.get(
                    "stale_after_minutes",
                    30,
                )
            )
            if audit_required
            else None
        )
        reader = Reader(
            self.spark,
            self.event_name,
            audit=audit,
            metadata_config=metadata,
            sql=sql,
        )
        source_files = reader.discover_files(metadata)
        partition_config = (metadata.get("source") or {}).get("partition") or {}
        batch_size = partition_config.get("files_per_batch", 100)
        max_files_per_run = partition_config.get("max_files_per_run", 500)
        if not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("source.partition.files_per_batch must be positive")
        if not isinstance(max_files_per_run, int) or max_files_per_run < 1:
            raise ValueError("source.partition.max_files_per_run must be positive")
        selected_files = source_files[:max_files_per_run]
        target = metadata.get("target") or {}
        target_table = target.get("table_name") or target.get("folder_name")
        if not target_table:
            raise ValueError("target.table_name or target.folder_name is required")
        source_path_column = (metadata.get("source") or {}).get(
            "source_path_column",
            "_source_gcs_path",
        )
        if source_path_column not in sql:
            raise ValueError(
                "Transformation SQL must retain the source path column "
                f"{source_path_column!r} for per-file audit metrics"
            )

        results = []
        total_rows_read = 0
        total_rows_written = 0
        for start in range(0, len(selected_files), batch_size):
            batch_candidates = selected_files[start : start + batch_size]
            if audit is None:
                claimed_files = batch_candidates
            else:
                claimed_ids = audit.claim_files(batch_candidates, target_table)
                claimed_files = [
                    {
                        **source_file,
                        "ingestion_id": claimed_ids[source_file["gcs_path"]],
                    }
                    for source_file in batch_candidates
                    if source_file["gcs_path"] in claimed_ids
                ]
            if not claimed_files:
                continue

            try:
                read_result = reader.read_files(claimed_files, metadata)
                if source_path_column not in read_result["dataframe"].columns:
                    raise ValueError(
                        f"Reader source path column is missing: {source_path_column}"
                    )
                input_counts = {
                    row[source_path_column]: row["count"]
                    for row in read_result["dataframe"]
                    .groupBy(col(source_path_column))
                    .count()
                    .collect()
                }
                transformed = Transformation(
                    dataframe=read_result["dataframe"],
                    metadata=metadata,
                    sql=read_result["sql"],
                    spark=self.spark,
                ).transform()
                validation = Validator(transformed, metadata).validate()
                if not validation["success"]:
                    raise ValueError(f"Validation failed: {validation['errors']}")
                if source_path_column not in transformed.columns:
                    raise ValueError(
                        "Transformation output must retain the source path column "
                        f"{source_path_column!r}"
                    )
                output_counts = {
                    row[source_path_column]: row["count"]
                    for row in transformed.groupBy(col(source_path_column))
                    .count()
                    .collect()
                }

                write_result = Writer(
                    dataframe=transformed,
                    metadata=metadata,
                    spark=self.spark,
                ).writer()

                audit_records = []
                claimed_by_path = {
                    source_file["gcs_path"]: source_file
                    for source_file in claimed_files
                }
                batch_results = []
                for path, source_file in claimed_by_path.items():
                    rows_read = input_counts.get(path, 0)
                    rows_written = output_counts.get(path, 0)
                    total_rows_read += rows_read
                    batch_results.append(
                        {
                            "source_gcs_path": path,
                            "event_id": source_file.get("event_id"),
                            "rows_read": rows_read,
                            "rows_written": rows_written,
                            "status": "SUCCESS",
                        }
                    )
                    if audit is not None:
                        audit_records.append(
                            {
                                "ingestion_id": source_file["ingestion_id"],
                                "rows_read": rows_read,
                                "rows_written": rows_written,
                            }
                        )
                if audit is not None:
                    audit.complete_files(audit_records)
                results.extend(batch_results)
                total_rows_written += write_result["rows_written"]
            except Exception as exc:
                if audit is not None:
                    try:
                        audit.fail_files(
                            [item["ingestion_id"] for item in claimed_files],
                            exc,
                        )
                    except Exception:
                        LOGGER.exception(
                            "Unable to record Silver pipeline failure for %s",
                            self.event_name,
                        )
                LOGGER.exception(
                    "Silver pipeline failed for %s",
                    self.event_name,
                )
                raise

        return {
            "status": "SUCCESS",
            "pipeline": self.event_name,
            "files_processed": len(results),
            "files_discovered": len(source_files),
            "files_deferred": len(source_files) - len(selected_files),
            "rows_read": total_rows_read,
            "rows_written": total_rows_written,
            "results": results,
        }
