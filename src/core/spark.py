import os
import logging
from pyspark.sql import SparkSession
from core.logging_config import get_logger

logger = get_logger(__name__)


def get_spark():
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("🔍 Environment Variables:")
        for key, value in os.environ.items():
            if any(spark_key in key.lower() for spark_key in ['spark', 'databricks', 'hadoop']):
                logger.debug(f"  📌 {key}: {value}")

    try:
        from databricks.connect import DatabricksSession
        logger.info("🚀 Attempting to create serverless Databricks session")
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        logger.warning("⚠️ Databricks Connect not available, trying local Spark session")
        return SparkSession.builder.getOrCreate()
    except Exception as ex:
        logger.warning(f"❌ Error creating serverless Databricks session: {str(ex)}")
        logger.warning("🔄 Falling back to local Spark session")
        return SparkSession.builder.getOrCreate()
