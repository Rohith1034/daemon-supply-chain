import argparse

from spark.framework.pipeline import Pipeline
from spark.framework.spark_session import create_spark


def run_pipeline(pipeline_name):
    spark = create_spark(app_name=f"silver-{pipeline_name}")
    try:
        return Pipeline(spark=spark, event_name=pipeline_name).run()
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run a metadata-driven Spark ETL job")
    parser.add_argument(
        "pipeline",
        help="Name of a YAML file under spark/configs, without the .yaml suffix",
    )
    args = parser.parse_args()

    print(run_pipeline(args.pipeline), flush=True)


if __name__ == "__main__":
    main()
