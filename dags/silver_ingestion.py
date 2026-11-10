from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("daemon-supply-chain-silver-ingestion")
    .master("spark://spark-master:7077")
    .config("spark.executor.instances", "3")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "3")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.ivy", "/opt/airflow/logs/.ivy2")
    .config(
        "spark.jars.packages",
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.20",
    )
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    )
    .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
    .config("spark.hadoop.io.file.buffer.size", "8388608")
    .config("spark.hadoop.fs.gs.inputstream.inplace.seek.limit", "8388608")
    .config("spark.hadoop.fs.gs.inputstream.min.range.request.size", "2097152")
    .config("spark.hadoop.fs.gs.inputstream.buffer.size", "8388608")
    .config("spark.hadoop.fs.gs.outputstream.buffer.size", "8388608")
    .getOrCreate()
)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set(
    "fs.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
)
hadoop_conf.set(
    "fs.AbstractFileSystem.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
)
hadoop_conf.set("fs.gs.auth.type", "APPLICATION_DEFAULT")
hadoop_conf.set("fs.gs.inputstream.inplace.seek.limit", "8388608")
hadoop_conf.set("fs.gs.inputstream.min.range.request.size", "2097152")
hadoop_conf.set("fs.gs.inputstream.buffer.size", "8388608")
hadoop_conf.set("fs.gs.outputstream.buffer.size", "8388608")
hadoop_conf.set("io.file.buffer.size", "8388608")

print(spark)