import json

from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from spark.framework.metadata import resolve_spark_path


class ValidationResult:
    def __init__(self):
        self.success = True
        self.errors = []
        self.metrics = {
            "rows_checked": 0,
            "null_failures": {},
            "duplicate_failures": {},
            "schema_failures": [],
        }

    def add_error(self, message):
        self.success = False
        self.errors.append(message)

    def to_dict(self):
        return {
            "success": self.success,
            "errors": self.errors,
            "metrics": self.metrics,
        }


class Validator:
    def __init__(self, dataframe, metadata):
        self.dataframe = dataframe
        self.metadata = metadata

    def _schema(self, schema_config):
        if not isinstance(schema_config, dict):
            return None
        schema_path = schema_config.get("schema_location")
        if not schema_path:
            return None
        path = resolve_spark_path(schema_path)
        if not path.is_file():
            raise FileNotFoundError(f"Spark validation schema not found: {path}")
        with path.open(encoding="utf-8") as file:
            try:
                return StructType.fromJson(json.load(file))
            except (TypeError, ValueError, KeyError) as exc:
                raise ValueError(f"Invalid Spark schema in {path}: {exc}") from exc

    @staticmethod
    def _quoted_column(name):
        return col(f"`{name.replace('`', '``')}`")

    def _check_columns(self, result, required_columns):
        missing = sorted(set(required_columns) - set(self.dataframe.columns))
        if missing:
            result.add_error(f"Required columns are missing: {missing}")

    def _check_nulls(self, result, columns):
        for name in sorted(set(columns)):
            if name not in self.dataframe.columns:
                result.add_error(f"Cannot check NULL; column is missing: {name}")
                continue
            null_count = self.dataframe.filter(
                self._quoted_column(name).isNull()
            ).count()
            if null_count:
                result.metrics["null_failures"][name] = null_count
                result.add_error(f"{name} contains {null_count} NULL values")

    def _check_uniqueness(self, result, columns, label):
        for name in sorted(set(columns)):
            if name not in self.dataframe.columns:
                result.add_error(f"Cannot check uniqueness; column is missing: {name}")
                continue
            duplicate_count = (
                self.dataframe.groupBy(self._quoted_column(name))
                .count()
                .filter(col("count") > 1)
                .count()
            )
            if duplicate_count:
                result.metrics["duplicate_failures"][label or name] = (
                    duplicate_count
                )
                result.add_error(
                    f"{label or name} has {duplicate_count} duplicate values"
                )

    def _check_primary_keys(self, result, primary_keys):
        if not primary_keys:
            return
        missing = sorted(set(primary_keys) - set(self.dataframe.columns))
        if missing:
            result.add_error(f"Primary key columns are missing: {missing}")
            return

        null_condition = self._quoted_column(primary_keys[0]).isNull()
        for key in primary_keys[1:]:
            null_condition = null_condition | self._quoted_column(key).isNull()
        null_count = self.dataframe.filter(null_condition).count()
        if null_count:
            result.metrics["null_failures"]["primary_keys"] = null_count
            result.add_error(f"Primary keys contain {null_count} NULL rows")

        duplicate_count = (
            self.dataframe.groupBy(
                *(self._quoted_column(key) for key in primary_keys)
            )
            .count()
            .filter(col("count") > 1)
            .count()
        )
        if duplicate_count:
            result.metrics["duplicate_failures"]["primary_keys"] = duplicate_count
            result.add_error(
                f"Primary key combination has {duplicate_count} duplicate groups"
            )

    def _check_schema(self, result, schema, config):
        if schema is None:
            return
        actual_fields = {field.name: field for field in self.dataframe.schema.fields}
        expected_fields = {field.name: field for field in schema.fields}

        missing = sorted(set(expected_fields) - set(actual_fields))
        if missing:
            result.metrics["schema_failures"].append(
                {"type": "missing_columns", "columns": missing}
            )

        allow_extra = config.get("allow_extra_columns", False)
        extra = sorted(set(actual_fields) - set(expected_fields))
        if extra and not allow_extra:
            result.metrics["schema_failures"].append(
                {"type": "unexpected_columns", "columns": extra}
            )

        type_mismatches = []
        for name in sorted(set(expected_fields) & set(actual_fields)):
            expected_type = expected_fields[name].dataType
            actual_type = actual_fields[name].dataType
            if expected_type != actual_type:
                type_mismatches.append(
                    {
                        "column": name,
                        "expected": expected_type.simpleString(),
                        "actual": actual_type.simpleString(),
                    }
                )
        if type_mismatches:
            result.metrics["schema_failures"].append(
                {"type": "type_mismatches", "columns": type_mismatches}
            )

        non_nullable = [
            field.name for field in schema.fields if not field.nullable
        ]
        result.metrics["schema_failures"].extend(
            {"type": "non_nullable", "columns": [name]}
            for name in non_nullable
            if name not in self.dataframe.columns
        )
        if result.metrics["schema_failures"]:
            result.add_error("DataFrame does not conform to the configured schema")

    def validate(self):
        if self.dataframe is None:
            raise ValueError("Validator requires a dataframe")
        if not isinstance(self.metadata, dict):
            raise ValueError("Validator requires pipeline metadata")

        result = ValidationResult()
        validation = self.metadata.get("validation") or {}
        processing = self.metadata.get("processing") or {}
        schema_validation = validation.get("schema") or {}
        if not isinstance(schema_validation, dict):
            schema_validation = {}
        schema = self._schema(schema_validation)
        if schema_validation.get("enabled") and schema is None:
            raise ValueError(
                "Schema validation is enabled but no validation.schema.schema_location "
                "is configured"
            )
        result.metrics["rows_checked"] = self.dataframe.count()

        schema_required = (
            [field.name for field in schema.fields if not field.nullable]
            if schema is not None
            else []
        )
        required = validation.get("required_columns") or []
        self._check_columns(result, [*required, *schema_required])
        self._check_schema(result, schema, schema_validation)

        self._check_nulls(
            result,
            [
                *(validation.get("not_null_columns") or []),
                *schema_required,
                *processing.get("primary_keys", []),
            ],
        )
        self._check_uniqueness(
            result,
            validation.get("unique_columns") or [],
            None,
        )
        self._check_primary_keys(
            result,
            processing.get("primary_keys") or [],
        )
        return result.to_dict()
