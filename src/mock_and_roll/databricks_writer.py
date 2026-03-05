"""Write generated pandas datasets directly to Delta tables on Databricks."""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from .spec import DataModelSpec, DatasetSpec


def _set_profile(profile: Optional[str]) -> None:
    if profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile


def _quoted(name: str) -> str:
    return f"`{name}`"


def write_dataset_to_delta(
    spec: DatasetSpec,
    dataframe: pd.DataFrame,
    mode: str = "overwrite",
    profile: str | None = None,
    create_catalog: bool = False,
    create_schema: bool = True,
) -> str:
    """
    Create a Delta table directly from generated data.

    This method intentionally skips volume ingestion and transformation stages.
    """
    _set_profile(profile)

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
    catalog = _quoted(spec.catalog)
    schema = _quoted(spec.schema_name)
    table = _quoted(spec.table)

    if create_catalog:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

    if create_schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark_df = spark.createDataFrame(dataframe)
    spark_df.write.format("delta").mode(mode).saveAsTable(f"{catalog}.{schema}.{table}")
    return spec.full_table_name()


def write_model_to_delta(
    model_spec: DataModelSpec,
    frames_by_dataset: dict[str, pd.DataFrame],
    mode: str = "overwrite",
    profile: str | None = None,
    create_catalog: bool = False,
    create_schema: bool = True,
) -> list[str]:
    """Write all datasets from a model spec to Delta tables."""
    _set_profile(profile)
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
    written_tables: list[str] = []

    for dataset in model_spec.datasets:
        frame = frames_by_dataset.get(dataset.name)
        if frame is None:
            continue

        catalog = _quoted(dataset.catalog)
        schema = _quoted(dataset.schema_name)
        table = _quoted(dataset.table)

        if create_catalog:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        if create_schema:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        spark.createDataFrame(frame).write.format("delta").mode(mode).saveAsTable(f"{catalog}.{schema}.{table}")
        written_tables.append(dataset.full_table_name())

    return written_tables
