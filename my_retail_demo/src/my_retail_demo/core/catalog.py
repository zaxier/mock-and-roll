"""Catalog and schema management utilities for Databricks."""

from typing import Optional

from pyspark.sql import SparkSession

from .logging_config import get_logger

logger = get_logger(__name__)


def catalog_exists(spark: SparkSession, catalog_name: str) -> bool:
    try:
        catalogs = spark.sql("SHOW CATALOGS").collect()
        return catalog_name in [row.catalog for row in catalogs]
    except Exception as e:
        logger.warning(f"Error checking catalog existence: {e}")
        return False


def schema_exists(spark: SparkSession, catalog_name: str, schema_name: str) -> bool:
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()
        return schema_name in [row.databaseName for row in schemas]
    except Exception as e:
        logger.warning(f"Error checking schema existence: {e}")
        return False


def volume_exists(
    spark: SparkSession, catalog_name: str, schema_name: str, volume_name: str
) -> bool:
    try:
        volumes = spark.sql(
            f"SHOW VOLUMES IN `{catalog_name}`.`{schema_name}`"
        ).collect()
        return volume_name in [row.volume_name for row in volumes]
    except Exception as e:
        logger.warning(f"Error checking volume existence: {e}")
        return False


def create_catalog(
    spark: SparkSession, catalog_name: str, comment: Optional[str] = None
) -> bool:
    try:
        if catalog_exists(spark, catalog_name):
            logger.info(f"Catalog '{catalog_name}' already exists")
            return True
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`{comment_clause}")
        logger.info(f"Successfully created catalog '{catalog_name}'")
        return True
    except Exception as e:
        logger.exception(f"Failed to create catalog '{catalog_name}': {e}")
        return False


def create_schema(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    comment: Optional[str] = None,
) -> bool:
    try:
        if schema_exists(spark, catalog_name, schema_name):
            logger.info(f"Schema '{catalog_name}.{schema_name}' already exists")
            return True
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`{comment_clause}"
        )
        logger.info(f"Successfully created schema '{catalog_name}.{schema_name}'")
        return True
    except Exception as e:
        logger.exception(
            f"Failed to create schema '{catalog_name}.{schema_name}': {e}"
        )
        return False


def create_volume(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    comment: Optional[str] = None,
) -> bool:
    try:
        if volume_exists(spark, catalog_name, schema_name, volume_name):
            logger.info(
                f"Volume '{catalog_name}.{schema_name}.{volume_name}' already exists"
            )
            return True
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`{comment_clause}"
        )
        logger.info(
            f"Successfully created volume '{catalog_name}.{schema_name}.{volume_name}'"
        )
        return True
    except Exception as e:
        logger.exception(
            f"Failed to create volume '{catalog_name}.{schema_name}.{volume_name}': {e}"
        )
        return False


def ensure_catalog_schema_volume(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    auto_create_catalog: bool = False,
    auto_create_schema: bool = True,
    auto_create_volume: bool = True,
) -> bool:
    """Ensure catalog, schema, and volume exist, creating them if allowed."""
    if not catalog_name or not schema_name or not volume_name:
        logger.error("Catalog, schema, and volume names cannot be None or empty")
        return False

    logger.info(
        f"Ensuring catalog/schema/volume: {catalog_name}.{schema_name}.{volume_name}"
    )

    if not catalog_exists(spark, catalog_name):
        if auto_create_catalog:
            if not create_catalog(spark, catalog_name, "Auto-created for demo"):
                return False
        else:
            logger.error(
                f"Catalog '{catalog_name}' does not exist and auto-creation is disabled"
            )
            return False

    if not schema_exists(spark, catalog_name, schema_name):
        if auto_create_schema:
            if not create_schema(
                spark, catalog_name, schema_name, "Auto-created for demo"
            ):
                return False
        else:
            logger.error(
                f"Schema '{catalog_name}.{schema_name}' does not exist and auto-creation is disabled"
            )
            return False

    if not volume_exists(spark, catalog_name, schema_name, volume_name):
        if auto_create_volume:
            if not create_volume(
                spark, catalog_name, schema_name, volume_name, "Auto-created for demo"
            ):
                return False
        else:
            logger.error(
                f"Volume '{catalog_name}.{schema_name}.{volume_name}' does not exist and auto-creation is disabled"
            )
            return False

    logger.info(
        f"All resources verified: {catalog_name}.{schema_name}.{volume_name}"
    )
    return True
