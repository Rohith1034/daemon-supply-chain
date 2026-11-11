import logging

from spark.framework.pipeline import Pipeline


LOGGER = logging.getLogger(__name__)


def run_pipeline(spark, pipeline_name):
    if spark is None:
        raise ValueError("A SparkSession is required")
    if not pipeline_name:
        raise ValueError("A pipeline name is required")

    LOGGER.info("Starting Silver pipeline %s", pipeline_name)
    result = Pipeline(spark=spark, event_name=pipeline_name).run()
    LOGGER.info("Silver pipeline %s completed: %s", pipeline_name, result)
    return result
