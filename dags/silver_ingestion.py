def main():
    from spark.framework.spark_session import create_spark

    spark = create_spark(app_name="daemon-supply-chain-silver-ingestion")
    try:
        print(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
