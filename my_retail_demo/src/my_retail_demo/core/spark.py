"""Spark session management."""

import os
import logging

from pyspark.sql import SparkSession

from .logging_config import get_logger

logger = get_logger(__name__)


def get_spark() -> SparkSession:
    """Get or create a Spark session. Tries Databricks Connect first, falls back to local."""
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Environment Variables:")
        for key, value in os.environ.items():
            if any(k in key.lower() for k in ["spark", "databricks", "hadoop"]):
                logger.debug(f"  {key}: {value}")

    try:
        from databricks.connect import DatabricksSession

        logger.info("Attempting to create serverless Databricks session")
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        logger.warning("Databricks Connect not available, trying local Spark session")
        return SparkSession.builder.getOrCreate()
    except Exception as ex:
        logger.warning(f"Error creating serverless Databricks session: {ex}")
        logger.warning("Falling back to local Spark session")
        return SparkSession.builder.getOrCreate()
