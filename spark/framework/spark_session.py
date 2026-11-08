from pyspark.sql import SparkSession
import os


def create_spark(app_name="daemon-supply-chain"):

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
    )

    # Local development
    if os.getenv("ENV", "local").lower() == "local":

        builder = (
            builder
            .master("local[*]")
            .config(
                "spark.hadoop.google.cloud.auth.type",
                "SERVICE_ACCOUNT_FILE",
            )
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.path.expanduser("~/daemon-supply-chain-sa.json"),
            )
        )

    # VM / Docker / Airflow
    else:

        builder = (
            builder
            .master("spark://spark-master:7077")
            .config("spark.executor.cores", "1")
            .config("spark.executor.memory", "1g")
            .config("spark.cores.max", "3")
            .config(
                "spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26",
            )
            .config(
                "spark.jars.ivy",
                "/opt/airflow/logs/.ivy2",
            )
            .config(
                "spark.hadoop.google.cloud.auth.type",
                "APPLICATION_DEFAULT",
            )
        )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark