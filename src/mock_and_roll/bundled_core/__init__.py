"""Core module for generated projects.

Usage:
    from .core import *
"""

# These modules have no pyspark dependency
from .data import Dataset, DataModel
from .logging_config import setup_logging, get_logger
from .config import Config, get_config, parse_args

# Pyspark-dependent modules: import eagerly when available, skip otherwise.
# In generated projects pyspark is always installed. During mock-and-roll
# development it may not be, and that's fine since only the scaffold/CLI
# code runs there.
try:
    from .spark import get_spark
    from .catalog import ensure_catalog_schema_volume
    from .io import (
        save_to_volume,
        save_datamodel_to_volume,
        batch_load_with_copy_into,
        batch_load_datamodel_from_volume,
        get_bronze_table_name,
    )
    from .workspace import get_workspace_schema_url
except ImportError:
    pass

__all__ = [
    "Dataset",
    "DataModel",
    "get_spark",
    "setup_logging",
    "get_logger",
    "Config",
    "get_config",
    "parse_args",
    "ensure_catalog_schema_volume",
    "save_to_volume",
    "save_datamodel_to_volume",
    "batch_load_with_copy_into",
    "batch_load_datamodel_from_volume",
    "get_bronze_table_name",
    "get_workspace_schema_url",
]
