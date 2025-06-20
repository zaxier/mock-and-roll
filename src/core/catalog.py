#!/usr/bin/env python3
"""
Catalog and schema management utilities for Databricks.
Provides functions to create catalogs, schemas, and volumes as needed.
"""

from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from core.logging_config import get_logger


logger = get_logger(__name__)


def catalog_exists(spark: SparkSession, catalog_name: str) -> bool:
    """
    Check if a catalog exists in Databricks.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog to check
        
    Returns:
        True if catalog exists, False otherwise
    """
    try:
        catalogs = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [row.catalog for row in catalogs]
        return catalog_name in catalog_names
    except Exception as e:
        logger.warning(f"Error checking catalog existence: {e}")
        return False


def schema_exists(spark: SparkSession, catalog_name: str, schema_name: str) -> bool:
    """
    Check if a schema exists in the specified catalog.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog
        schema_name: Name of the schema to check
        
    Returns:
        True if schema exists, False otherwise
    """
    try:
        schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()
        schema_names = [row.databaseName for row in schemas]
        return schema_name in schema_names
    except Exception as e:
        logger.warning(f"Error checking schema existence: {e}")
        return False


def volume_exists(spark: SparkSession, catalog_name: str, schema_name: str, volume_name: str) -> bool:
    """
    Check if a volume exists in the specified catalog and schema.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        volume_name: Name of the volume to check
        
    Returns:
        True if volume exists, False otherwise
    """
    try:
        volumes = spark.sql(f"SHOW VOLUMES IN `{catalog_name}`.`{schema_name}`").collect()
        volume_names = [row.volume_name for row in volumes]
        return volume_name in volume_names
    except Exception as e:
        logger.warning(f"Error checking volume existence: {e}")
        return False


def create_catalog(spark: SparkSession, catalog_name: str, comment: Optional[str] = None) -> bool:
    """
    Create a catalog in Databricks.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog to create
        comment: Optional comment for the catalog
        
    Returns:
        True if catalog was created or already exists, False if creation failed
    """
    try:
        if catalog_exists(spark, catalog_name):
            logger.info(f"Catalog '{catalog_name}' already exists")
            return True
        
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        create_sql = f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`{comment_clause}"
        
        logger.info(f"Creating catalog: {catalog_name}")
        spark.sql(create_sql)
        logger.info(f"✅ Successfully created catalog '{catalog_name}'")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create catalog '{catalog_name}': {e}")
        return False


def create_schema(spark: SparkSession, catalog_name: str, schema_name: str, comment: Optional[str] = None) -> bool:
    """
    Create a schema in the specified catalog.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog
        schema_name: Name of the schema to create
        comment: Optional comment for the schema
        
    Returns:
        True if schema was created or already exists, False if creation failed
    """
    try:
        if schema_exists(spark, catalog_name, schema_name):
            logger.info(f"Schema '{catalog_name}.{schema_name}' already exists")
            return True
        
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        create_sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`{comment_clause}"
        
        logger.info(f"Creating schema: {catalog_name}.{schema_name}")
        spark.sql(create_sql)
        logger.info(f"✅ Successfully created schema '{catalog_name}.{schema_name}'")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create schema '{catalog_name}.{schema_name}': {e}")
        return False


def create_volume(spark: SparkSession, catalog_name: str, schema_name: str, volume_name: str, 
                 comment: Optional[str] = None) -> bool:
    """
    Create a volume in the specified catalog and schema.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        volume_name: Name of the volume to create
        comment: Optional comment for the volume
        
    Returns:
        True if volume was created or already exists, False if creation failed
    """
    try:
        if volume_exists(spark, catalog_name, schema_name, volume_name):
            logger.info(f"Volume '{catalog_name}.{schema_name}.{volume_name}' already exists")
            return True
        
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        create_sql = f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`{comment_clause}"
        
        logger.info(f"Creating volume: {catalog_name}.{schema_name}.{volume_name}")
        spark.sql(create_sql)
        logger.info(f"✅ Successfully created volume '{catalog_name}.{schema_name}.{volume_name}'")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create volume '{catalog_name}.{schema_name}.{volume_name}': {e}")
        return False


def ensure_catalog_schema_volume(spark: SparkSession, catalog_name: str, schema_name: str, 
                                volume_name: str, auto_create_catalog: bool = False, 
                                auto_create_schema: bool = True, auto_create_volume: bool = True) -> bool:
    """
    Ensure catalog, schema, and volume exist, creating them if allowed by configuration.
    
    Args:
        spark: Active Spark session
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        volume_name: Name of the volume
        auto_create_catalog: Whether to auto-create catalog if it doesn't exist
        auto_create_schema: Whether to auto-create schema if it doesn't exist
        auto_create_volume: Whether to auto-create volume if it doesn't exist
        
    Returns:
        True if all resources exist or were created successfully, False otherwise
    """
    # Handle None values
    if not catalog_name or not schema_name or not volume_name:
        logger.error("❌ Catalog, schema, and volume names cannot be None or empty")
        return False
        
    logger.info(f"Ensuring catalog/schema/volume: {catalog_name}.{schema_name}.{volume_name}")
    
    # Check catalog
    if not catalog_exists(spark, catalog_name):
        if auto_create_catalog:
            if not create_catalog(spark, catalog_name, f"Auto-created catalog for demo purposes"):
                return False
        else:
            logger.error(f"❌ Either catalog '{catalog_name}' does not exist and auto-creation is disabled or we ran into Spark error such as NO_ACTIVE_SESSION ")
            logger.info("To enable catalog auto-creation, set auto_create_catalog: true in your config")
            return False
    
    # Check schema
    if not schema_exists(spark, catalog_name, schema_name):
        if auto_create_schema:
            if not create_schema(spark, catalog_name, schema_name, f"Auto-created schema for demo purposes"):
                return False
        else:
            logger.error(f"❌ Schema '{catalog_name}.{schema_name}' does not exist and auto-creation is disabled")
            logger.info("To enable schema auto-creation, set auto_create_schema: true in your config")
            return False
    
    # Check volume
    if not volume_exists(spark, catalog_name, schema_name, volume_name):
        if auto_create_volume:
            if not create_volume(spark, catalog_name, schema_name, volume_name, f"Auto-created volume for demo data"):
                return False
        else:
            logger.error(f"❌ Volume '{catalog_name}.{schema_name}.{volume_name}' does not exist and auto-creation is disabled")
            logger.info("To enable volume auto-creation, set auto_create_volume: true in your config")
            return False
    
    logger.info(f"✅ All resources verified: {catalog_name}.{schema_name}.{volume_name}")
    return True