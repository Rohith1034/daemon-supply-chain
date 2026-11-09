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
        # Read Parquet files in batches
        # -----------------------------
        files_per_batch = 15
        dataframes = []

        for event in event_types:
            prefix = f"{base_path}/{event}/"
            files = [
                f"gs://{bucket_name}/{blob.name}"
                for blob in storage_client.list_blobs(
                    bucket_name,
                    prefix=prefix,
                )
                if blob.name.endswith(".parquet")
            ]

            if not files:
                raise FileNotFoundError(
                    f"No Parquet files found in gs://{bucket_name}/{prefix}"
                )

            for start in range(0, len(files), files_per_batch):
                batch = files[start : start + files_per_batch]
                batch_number = start // files_per_batch + 1
                print(
                    f"Reading event {event}: files "
                    f"{start + 1}-{start + len(batch)} of {len(files)} "
                    f"(batch {batch_number})",
                    flush=True,
                )

                df = self.spark.read.parquet(*batch)
                print(f"Sample rows for event {event}, batch {batch_number}:",
                      flush=True)
                df.show(20, truncate=False)
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

# print(
#     "fs.gs.impl =",
#     spark.sparkContext._jsc.hadoopConfiguration().get("fs.gs.impl")
# )

# print(
#     "fs.AbstractFileSystem.gs.impl =",
#     spark.sparkContext._jsc.hadoopConfiguration().get(
#         "fs.AbstractFileSystem.gs.impl"
#     )
# )

reader = Reader(
    spark=spark,
    event_name="purchase_order"
)

result = reader.reader()
print(result)

# print(
#     "fs.gs.impl =",
#     spark.sparkContext._jsc.hadoopConfiguration().get("fs.gs.impl")
# )

# print(
#     "fs.AbstractFileSystem.gs.impl =",
#     spark.sparkContext._jsc.hadoopConfiguration().get(
#         "fs.AbstractFileSystem.gs.impl"
#     )
# )

# df = result["dataframe"]
#
# metadata = result["metadata"]
#
# sql = result["sql"]