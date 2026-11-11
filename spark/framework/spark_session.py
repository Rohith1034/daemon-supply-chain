from pyspark.sql import SparkSession
import os


def create_spark(app_name="daemon-supply-chain"):
    environment = os.getenv("ENV", "local").lower()
    airflow_executor = os.getenv("AIRFLOW__CORE__EXECUTOR")
    master_url = os.getenv("SPARK_MASTER_URL")
    gcs_connector = os.getenv(
        "SPARK_GCS_CONNECTOR_PACKAGE",
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.20",
    )
    credential_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    gcs_auth_type = os.getenv(
        "GCS_AUTH_TYPE",
        "SERVICE_ACCOUNT_JSON_KEYFILE"
        if credential_file
        else "APPLICATION_DEFAULT",
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", gcs_connector)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.io.file.buffer.size", "8388608")
        .config("spark.hadoop.fs.gs.inputstream.inplace.seek.limit", "8388608")
        .config("spark.hadoop.fs.gs.inputstream.min.range.request.size", "2097152")
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
        .config(
            "spark.hadoop.fs.gs.auth.type",
            gcs_auth_type,
        )
    )
    if credential_file:
        builder = builder.config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            credential_file,
        )

    if master_url:
        builder = builder.master(master_url)
    elif environment == "local" and not airflow_executor:
        builder = builder.master("local[*]")

    # VM / Docker / Airflow
    else:

        builder = (
            builder
            .master("spark://spark-master:7077")
            .config("spark.executor.instances", "3")
            .config("spark.executor.cores", "1")
            .config("spark.executor.memory", "1g")
            .config("spark.cores.max", "3")
            .config(
                "spark.jars.ivy",
                "/opt/airflow/logs/.ivy2",
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
    hadoop_conf.set("fs.gs.block.size", "67108864")
    hadoop_conf.set("fs.gs.inputstream.buffer.size", "8388608")
    hadoop_conf.set("fs.gs.outputstream.buffer.size", "8388608")
    hadoop_conf.set("fs.gs.inputstream.inplace.seek.limit", "8388608")
    hadoop_conf.set("fs.gs.inputstream.min.range.request.size", "2097152")
    hadoop_conf.set("io.file.buffer.size", "8388608")

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
