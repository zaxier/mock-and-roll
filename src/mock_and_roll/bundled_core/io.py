"""Data I/O operations: save to volume, batch load with COPY INTO."""

from typing import List, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .config import Config
from .data import DataModel
from .logging_config import get_logger

logger = get_logger(__name__)


def get_bronze_table_name(dataset_name: str) -> str:
    """Generate consistent bronze table name with _bronze suffix."""
    return f"{dataset_name}_bronze"


def save_to_volume(
    spark: SparkSession,
    df: pd.DataFrame | DataFrame,
    file_path: str,
    file_format: str = "parquet",
) -> None:
    """Save pandas or PySpark DataFrame to a volume path."""
    if isinstance(df, pd.DataFrame):
        df = spark.createDataFrame(df)

    df.write.mode("overwrite").format(file_format).save(file_path)


def batch_load_with_copy_into(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    file_format: str = "PARQUET",
    table_schema: Optional[StructType] = None,
    drop_table_if_exists: bool = False,
    copy_options: Optional[dict] = None,
) -> DataFrame:
    """Batch load data using COPY INTO command."""
    try:
        if drop_table_if_exists:
            logger.debug(f"Dropping table if exists: {target_table}")
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        if table_schema is not None:
            table_parts = target_table.split(".")
            if len(table_parts) == 3:
                catalog_name, schema_name, table_name = table_parts
                result = spark.sql(
                    f"SHOW TABLES IN {catalog_name}.{schema_name} LIKE '{table_name}'"
                )
            elif len(table_parts) == 2:
                schema_name, table_name = table_parts
                result = spark.sql(
                    f"SHOW TABLES IN {schema_name} LIKE '{table_name}'"
                )
            else:
                table_name = target_table
                result = spark.sql(f"SHOW TABLES LIKE '{table_name}'")

            if result.count() == 0:
                logger.debug(f"Creating table {target_table} with provided schema")
                ddl_fields = []
                for f in table_schema.fields:
                    field_type = f.dataType.simpleString().upper()
                    nullable = "" if f.nullable else " NOT NULL"
                    ddl_fields.append(f"{f.name} {field_type}{nullable}")
                ddl_string = ", ".join(ddl_fields)
                spark.sql(f"CREATE TABLE {target_table} ({ddl_string})")

        copy_sql = (
            f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = {file_format}"
        )
        if copy_options:
            options_str = ", ".join(
                [f"{k} = '{v}'" for k, v in copy_options.items()]
            )
            copy_sql += f" OPTIONS ({options_str})"

        logger.debug(f"Executing COPY INTO: {copy_sql}")
        spark.sql(copy_sql)

        logger.info(f"Successfully loaded data into {target_table}")
        return spark.sql(f"SELECT * FROM {target_table}")

    except Exception as e:
        logger.error(f"Error in batch_load_with_copy_into: {e}")
        raise


def save_datamodel_to_volume(
    spark: SparkSession,
    data_model: DataModel,
    config: Config,
    base_subdirectory: str = "raw",
) -> List[str]:
    """Save all datasets in a DataModel to volume."""
    saved_paths = []
    for dataset in data_model.datasets:
        volume_path = config.get_volume_path(
            f"{base_subdirectory}/{dataset.get_file_path('')}"
        )
        logger.debug(f"Saving dataset '{dataset.name}' to {volume_path}")
        save_to_volume(spark=spark, df=dataset.data, file_path=volume_path)
        saved_paths.append(volume_path)
        logger.info(f"Successfully saved {dataset.name}")
    return saved_paths


def batch_load_datamodel_from_volume(
    spark: SparkSession,
    data_model: DataModel,
    config: Config,
    source_subdirectory: str = "raw",
    drop_tables_if_exist: bool = False,
) -> List[DataFrame]:
    """Batch load all datasets from volume to Delta tables (bronze layer)."""
    loaded_dfs = []
    for dataset in data_model.datasets:
        source_path = config.get_volume_path(
            f"{source_subdirectory}/{dataset.get_file_path('')}"
        )
        bronze_name = get_bronze_table_name(dataset.name)
        target_table = f"{config.catalog}.{config.schema}.{bronze_name}"

        logger.debug(
            f"Loading dataset '{dataset.name}' from {source_path} to {target_table}"
        )
        table_schema = spark.createDataFrame(dataset.data).schema

        df = batch_load_with_copy_into(
            spark=spark,
            source_path=source_path,
            target_table=target_table,
            table_schema=table_schema,
            drop_table_if_exists=drop_tables_if_exist,
        )
        loaded_dfs.append(df)
        logger.info(f"Successfully loaded {dataset.name}")
    return loaded_dfs
