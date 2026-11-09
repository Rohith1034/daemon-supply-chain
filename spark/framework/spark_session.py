from pyspark.sql import SparkSession
import os

GCS_CONNECTOR_PACKAGE = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.20"


def create_spark(app_name="daemon-supply-chain"):

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", GCS_CONNECTOR_PACKAGE)
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        .config(
            "spark.hadoop.fs.gs.block.size",
            "67108864"
        )
        .config(
            "spark.hadoop.fs.gs.inputstream.buffer.size",
            "8388608"
        )
        .config(
            "spark.hadoop.fs.gs.outputstream.buffer.size",
            "8388608"
        )
    )

    # Local development
    if os.getenv("ENV", "local").lower() == "local":

        builder = (
            builder
            .master("local[*]")
            .appName(app_name)

            .config(
                "spark.sql.shuffle.partitions",
                "4"
            )

            .config(
                "spark.sql.session.timeZone",
                "UTC"
            )

            .config(
                "spark.ui.enabled",
                "false"
            )
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
            )

            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            )

            # Use ADC
            .config(
                "spark.hadoop.fs.gs.auth.type",
                "APPLICATION_DEFAULT"
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
                "spark.jars.ivy",
                "/opt/airflow/logs/.ivy2",
            )
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.path.expanduser("~/daemon-supply-chain-sa.json"),
            )

        )
    spark = builder.getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set(
        "fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    hadoop_conf.set(
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark

# spark.range(5).show()
#
# spark.stop()

# spark = create_spark()
#
# print("Spark:", spark.version)
#
# print("\nSpark Conf")
# for k, v in sorted(spark.sparkContext.getConf().getAll()):
#     if "64" in str(v).lower() or "m" in str(v).lower():
#         print(k, "=", v)
#
# print("\nHadoop Conf")
# hc = spark.sparkContext._jsc.hadoopConfiguration()
#
# it = hc.iterator()
#
# while it.hasNext():
#     e = it.next()
#
#     if "64m" in str(e.getValue()).lower():
#         print(e.getKey(), "=", e.getValue())
# conf = spark.sparkContext._jsc.hadoopConfiguration()
#
# print("GCS block size:")
# print(conf.get("fs.gs.block.size"))
#
# print("Hadoop version:")
# print(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

# import os
# print(os.environ.get("JAVA_HOME"))
#
# import pyspark
# print(pyspark.__version__)
# conf = spark.sparkContext._jsc.hadoopConfiguration()
#
# iterator = conf.iterator()
#
# while iterator.hasNext():
#     item = iterator.next()
#     key = item.getKey()
#     value = item.getValue()
#
#     if "8m" in value.lower() or "memory" in key.lower() or "size" in key.lower():
#         print(key, "=", value)
