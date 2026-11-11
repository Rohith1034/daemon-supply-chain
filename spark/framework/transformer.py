from spark.framework.audit import Audit
from spark.framework.metadata import load_metadata
from spark.framework.pipeline import Pipeline
from spark.framework.reader import Reader
from spark.framework.transformation import Transformation


class Transformer:
    def __init__(self, spark, event_name):
        self.spark = spark
        self.event_name = event_name

    def transform(self):
        metadata, sql = load_metadata(self.event_name)
        audit_enabled = (metadata.get("audit") or {}).get("enabled", True)
        skip_processed = (metadata.get("checkpoint") or {}).get(
            "skip_processed_files", True
        )
        reader = Reader(
            spark=self.spark,
            event_name=self.event_name,
            audit=Audit() if audit_enabled or skip_processed else None,
            metadata_config=metadata,
            sql=sql,
        )
        result = reader.reader()
        dataframe = Transformation(
            result["dataframe"],
            result["metadata"],
            result["sql"],
            self.spark,
        ).transform()
        return {
            "dataframe": dataframe,
            "metadata": result["metadata"],
            "spark": self.spark,
        }


def main_transformer(spark, event_name):
    return Pipeline(spark=spark, event_name=event_name).run()
