import re


class Transformation:
    def __init__(self, dataframe, metadata, sql, spark):
        self.dataframe = dataframe
        self.metadata = metadata
        self.sql = sql
        self.spark = spark

    def transform(self):
        if self.dataframe is None:
            raise ValueError("Transformation requires a dataframe")
        if not isinstance(self.metadata, dict):
            raise ValueError("Transformation requires pipeline metadata")
        if not self.sql or not self.sql.strip():
            raise ValueError("Transformation SQL is empty")

        view_name = (
            (self.metadata.get("processing") or {})
            .get("source_view", "source_dataframe")
        )
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", view_name):
            raise ValueError(f"Invalid temporary view name: {view_name!r}")

        self.dataframe.createOrReplaceTempView(view_name)
        return self.spark.sql(self.sql)
