from pyspark.sql import SparkSession
from spark.framework.metadata import metadata
from google.cloud import storage

class Reader:
    def __init__(self, event_name):
        self.event_name = event_name
    def create_spark_session(self):
        spark = None
        if spark is None:
            spark = (
                SparkSession.builder
                .appName("daemon-supply-chain")
                .master("spark://spark-master:7077")
                .config("spark.executor.cores", "1")
                .config("spark.executor.memory", "1g")
                .config("spark.cores.max", "3")
                .config("spark.sql.shuffle.partitions", "6")
                .config("spark.sql.session.timeZone", "UTC")
                .config(
                    "spark.jars.packages",
                    "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26",
                )
                .config(
                    "spark.jars.ivy",
                    "/opt/airflow/logs/.ivy2",
                )
                .config(
                    "spark.hadoop.fs.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                )
                .config(
                    "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                )
                .config(
                    "spark.hadoop.google.cloud.auth.type",
                    "APPLICATION_DEFAULT",
                )
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
        return spark

    def reader(self):
        meta = metadata(event_name=self.event_name)
        meta_config = meta[0]
        meta_sql = meta[1]
        if (meta_config is None or meta_config == "") or (meta_sql is None or meta_sql == ""):
            return "Invalid meta details"
        # print(meta_config)
        # print(meta_sql)
        source_details = meta_config.get("source")
        bucket_name = source_details.get("bucket_name")
        base_path = source_details.get("base_path")
        event_types = source_details.get("event_types")
        topic_name = source_details.get("")
        if bucket_name is None or bucket_name == "" or base_path is None or base_path == "" or event_types is None or event_types == []:
            return "invalid source details"

        storage_client = storage.Client()
        if storage_client.bucket(bucket_name).exists():
            "No such bucket exists"

        spark = (SparkSession.builder
         .master("local[*]")
         .appName("DataProcessingApp")
         .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22")
         .getOrCreate())

        folder_path = f"gs://{bucket_name}/{base_path}/{event_types[0]}"

        df = spark.read \
            .option("recursiveFileLookup", "true") \
            .parquet(folder_path)

        # Add this at the bottom of reader.py to force an output display
        print(f"Total rows read: {df.count()}")
        df.show(5, truncate=False)

        return None


read = Reader(event_name="purchase_order")
read.reader()

