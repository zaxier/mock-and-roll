from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType
import pandas as pd
from typing import List, Optional

import dlt
from .data import Dataset, DataModel
from config.settings import Config
from core.logging_config import get_logger


logger = get_logger(__name__)

# TODO: Implement IO functions for the medallion architecture.

# Save pandas or spark dataframe to volume with specified file path and file type: parquet, csv, json, etc.
def save_to_volume(spark: SparkSession, df: pd.DataFrame | DataFrame, file_path: str, file_format: str = "parquet"):
    """
    Save pandas or spark dataframe to volume with specified file path and file type: parquet, csv, json, etc.
    
    Args:
        spark: SparkSession instance
        df: Pandas or PySpark DataFrame to save
        file_path: Target path to save the file
        file_format: Format to save the file in (default: parquet)
    """
    # Convert pandas DataFrame to PySpark if needed
    if isinstance(df, pd.DataFrame):
        df = spark.createDataFrame(df)
    
    # Write the DataFrame using Spark's optimizeWrite feature
    (df.write
       .mode("overwrite")
       .format(file_format)
       .save(file_path))

# ReadStream using autoloader the data from volume with specified file path and file type: parquet, csv, json, etc.
def read_stream_with_autoloader(spark: SparkSession, source_path: str, schema, file_format: str = "parquet"):
        """
        Read data stream using Autoloader with metadata columns.
        
        Args:
            spark: SparkSession instance
            source_path: Path to source data
            schema: Schema for the data
            file_format: Format of source files (default: parquet)
            
        Returns:
            DataFrame with added metadata columns
        """
        # Ensure path starts with dbfs:/
        if not source_path.startswith('dbfs:/'):
            source_path = f"dbfs:{source_path}"
            
        # Read with Autoloader
        df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", file_format)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaLocation", f"{source_path}/_schema")
              .option("cloudFiles.partitionColumns", "")  # No partition columns for this use case
              .schema(schema)
              .load(source_path))
        
        # Add metadata columns
        return (df
                .withColumn("_ingestion_timestamp", current_timestamp())
                .withColumn("_source_file", col("_metadata.file_path")))

def write_stream_to_delta(df: DataFrame, target_table: str, checkpoint_path: str):
    """
    Write stream to Delta table using streaming.
    """
    query =(df.writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", checkpoint_path)
     .option("mergeSchema", "true")
     .trigger(availableNow=True)
     .toTable(target_table))
    return query

def read_stream_with_dlt(source_path: str, file_format: str = "parquet", schema=None, schema_location: str = None):
    """
    Read data stream using DLT with Auto Loader and metadata columns.
    Generic function for use within DLT views and tables.
    
    Args:
        source_path: Path to source data (must start with 'dbfs:/')
        file_format: Format of source files (default: parquet)
        schema: Optional schema for the data. If None, uses schema inference
        schema_location: Optional path for schema inference checkpoint
        
    Returns:
        DataFrame with added metadata columns for use in DLT pipelines
    """
    # Ensure path starts with dbfs:/
    if not source_path.startswith('dbfs:/'):
        raise ValueError("source_path must start with 'dbfs:/'")
    
    # Get Spark session from DLT context
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    # Build readStream with cloudFiles format
    read_stream = (spark.readStream
                   .format("cloudFiles")
                   .option("cloudFiles.format", file_format))
    
    # Add schema or enable schema inference
    if schema is not None:
        read_stream = read_stream.schema(schema)
    elif schema_location is not None:
        read_stream = read_stream.option("cloudFiles.schemaLocation", schema_location)
    else:
        # Use default schema inference without explicit location
        read_stream = read_stream.option("cloudFiles.inferColumnTypes", "true")
    
    # Load the data
    df = read_stream.load(source_path)
    
    # Add metadata columns
    return (df
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path")))

def batch_load_with_copy_into(
    spark: SparkSession, 
    source_path: str, 
    target_table: str, 
    file_format: str = "PARQUET",
    table_schema: StructType = None,
    drop_table_if_exists: bool = False,
    copy_options: dict = None
):
    """
    Batch load data using COPY INTO command for efficient data loading.
    
    Args:
        spark: SparkSession instance
        source_path: Path to source data files (supports wildcards)
        target_table: Target table name (catalog.schema.table)
        file_format: Source file format (PARQUET, JSON, CSV, etc.)
        table_schema: Optional StructType schema for table creation
        drop_table_if_exists: Whether to drop the table if it already exists
        copy_options: Additional COPY INTO options as key-value pairs
        
    Returns:
        DataFrame: Result of SELECT * FROM target_table after COPY INTO
    """
    
    try:
        # Drop table if requested
        if drop_table_if_exists:
            logger.debug(f"Dropping table if exists: {target_table}")
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        
        # Create table if schema is provided and table doesn't exist
        if table_schema is not None:
            try:
                # Check if table exists using SHOW TABLES
                table_parts = target_table.split('.')
                if len(table_parts) == 3:
                    catalog_name, schema_name, table_name = table_parts
                    result = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name} LIKE '{table_name}'")
                elif len(table_parts) == 2:
                    schema_name, table_name = table_parts
                    result = spark.sql(f"SHOW TABLES IN {schema_name} LIKE '{table_name}'")
                else:
                    table_name = target_table
                    result = spark.sql(f"SHOW TABLES LIKE '{table_name}'")
                
                table_exists = result.count() > 0
                
                if table_exists:
                    logger.debug(f"Table {target_table} already exists")
                else:
                    # Table doesn't exist, create it
                    logger.debug(f"Creating table {target_table} with provided schema")
                    
                    # Convert StructType to DDL string
                    ddl_fields = []
                    for field in table_schema.fields:
                        field_type = field.dataType.simpleString().upper()
                        nullable = "" if field.nullable else " NOT NULL"
                        ddl_fields.append(f"{field.name} {field_type}{nullable}")
                    
                    ddl_string = ", ".join(ddl_fields)
                    create_table_sql = f"CREATE TABLE {target_table} ({ddl_string})"
                    
                    logger.debug(f"Executing: {create_table_sql}")
                    spark.sql(create_table_sql)
                    
            except Exception as e:
                logger.error(f"Error checking/creating table: {str(e)}")
                raise
        
        # Build COPY INTO command
        copy_sql = f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = {file_format}"
        
        # Add additional options if provided
        if copy_options:
            options_str = ", ".join([f"{k} = '{v}'" for k, v in copy_options.items()])
            copy_sql += f" OPTIONS ({options_str})"
        
        logger.debug(f"Executing COPY INTO: {copy_sql}")
        spark.sql(copy_sql)
        
        # Return the loaded data
        logger.info(f"Successfully loaded data into {target_table}")
        return spark.sql(f"SELECT * FROM {target_table}")
        
    except Exception as e:
        logger.error(f"Error in batch_load_with_copy_into: {str(e)}")
        raise


def save_datamodel_to_volume(
    spark: SparkSession, 
    data_model: DataModel, 
    config: Config,
    base_subdirectory: str = "raw"
) -> List[str]:
    """
    Save all datasets in a DataModel to volume using configuration.
    
    Args:
        spark: SparkSession instance
        data_model: DataModel containing datasets to save
        config: Configuration object with volume settings
        base_subdirectory: Base subdirectory within volume (e.g., "raw", "bronze")
        
    Returns:
        List of saved file paths
    """
    saved_paths = []
    
    for dataset in data_model.datasets:
        # Build volume path using config
        volume_path = config.databricks.get_volume_path(
            f"{base_subdirectory}/{dataset.get_file_path('')}"
        )
        
        logger.debug(f"Saving dataset '{dataset.name}' to {volume_path}")
        
        save_to_volume(
            spark=spark,
            df=dataset.data,
            file_path=volume_path,
            file_format=dataset.file_format
        )
        
        saved_paths.append(volume_path)
        logger.info(f"Successfully saved {dataset.name}")
    
    return saved_paths


def batch_load_datamodel_from_volume(
    spark: SparkSession,
    data_model: DataModel,
    config: Config,
    source_subdirectory: str = "raw",
    target_schema_suffix: str = "_bronze",
    drop_tables_if_exist: bool = False
) -> List[DataFrame]:
    """
    Batch load all datasets from volume to Delta tables using COPY INTO.
    
    Args:
        spark: SparkSession instance  
        data_model: DataModel with dataset definitions
        config: Configuration object
        source_subdirectory: Source subdirectory in volume
        target_schema_suffix: Suffix for target table names
        drop_tables_if_exist: Whether to drop existing tables
        
    Returns:
        List of loaded DataFrames
    """
    loaded_dfs = []
    
    for dataset in data_model.datasets:
        # Source path in volume
        source_path = config.databricks.get_volume_path(
            f"{source_subdirectory}/{dataset.get_file_path('')}"
        )
        
        # Target table name
        target_table = f"{config.databricks.catalog}.{config.databricks.schema}.{dataset.name}{target_schema_suffix}"
        
        logger.debug(f"Loading dataset '{dataset.name}' from {source_path} to {target_table}")
        
        # Infer schema from the pandas DataFrame
        table_schema = spark.createDataFrame(dataset.data).schema
        
        # Load using COPY INTO
        df = batch_load_with_copy_into(
            spark=spark,
            source_path=source_path,
            target_table=target_table,
            file_format=dataset.file_format.upper(),
            table_schema=table_schema,
            drop_table_if_exists=drop_tables_if_exist
        )
        
        loaded_dfs.append(df)
        logger.info(f"Successfully loaded {dataset.name}")
    
    return loaded_dfs