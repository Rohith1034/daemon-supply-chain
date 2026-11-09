from google.cloud import storage
from spark.framework.spark_session import create_spark

from spark.framework.metadata import metadata


class Reader:

    def __init__(self, spark, event_name):
        self.spark = spark
        self.event_name = event_name

    def reader(self):

        # -----------------------------
        # Read metadata
        # -----------------------------
        meta_config, meta_sql = metadata(event_name=self.event_name)

        if not meta_config:
            raise ValueError("Metadata configuration not found.")

        source = meta_config.get("source", {})

        bucket_name = source.get("bucket_name")
        base_path = source.get("base_path")
        event_types = source.get("event_types")

        if not bucket_name:
            raise ValueError("bucket_name missing in yaml")

        if not base_path:
            raise ValueError("base_path missing in yaml")

        if not event_types:
            raise ValueError("event_types missing in yaml")

        # -----------------------------
        # Verify bucket exists
        # -----------------------------
        storage_client = storage.Client()

        if not storage_client.bucket(bucket_name).exists():
            raise FileNotFoundError(f"GCS bucket not found : {bucket_name}")

        # -----------------------------
        # Read all configured event types
        # -----------------------------
        dataframes = []

        for event in event_types:

            folder = f"gs://{bucket_name}/{base_path}/{event}"

            df = (
                self.spark.read
                .option("recursiveFileLookup", "true")
                .parquet(folder)
            )

            dataframes.append(df)

        # -----------------------------
        # Merge all event DataFrames
        # -----------------------------
        df = dataframes[0]

        for next_df in dataframes[1:]:
            df = df.unionByName(next_df, allowMissingColumns=True)

        return {
            "dataframe": df,
            "metadata": meta_config,
            "sql": meta_sql
        }




spark = create_spark()

print(
    "fs.gs.impl =",
    spark.sparkContext._jsc.hadoopConfiguration().get("fs.gs.impl")
)

print(
    "fs.AbstractFileSystem.gs.impl =",
    spark.sparkContext._jsc.hadoopConfiguration().get(
        "fs.AbstractFileSystem.gs.impl"
    )
)

reader = Reader(
    spark=spark,
    event_name="purchase_order"
)

result = reader.reader()
print(result)

print(
    "fs.gs.impl =",
    spark.sparkContext._jsc.hadoopConfiguration().get("fs.gs.impl")
)

print(
    "fs.AbstractFileSystem.gs.impl =",
    spark.sparkContext._jsc.hadoopConfiguration().get(
        "fs.AbstractFileSystem.gs.impl"
    )
)

# df = result["dataframe"]
#
# metadata = result["metadata"]
#
# sql = result["sql"]