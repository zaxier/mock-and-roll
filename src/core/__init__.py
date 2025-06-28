"""
Core module for AI-Native Databricks Demo Framework

This module provides a simplified import interface for all core functionality,
allowing users to import all functions and classes directly from the core module
rather than from individual submodules.

Usage:
    from core import *
"""

# Data Models
from .data import Dataset, DataModel

# Spark Session Management
from .spark import get_spark

# Logging Configuration
from .logging_config import setup_logging, get_logger

# CLI Argument Parsing
from .cli import parse_demo_args, get_demo_args_with_config

# Databricks Catalog Management
from .catalog import (
    ensure_catalog_schema_volume,
    # catalog_exists,
    # schema_exists,
    # volume_exists,
    # create_catalog,
    # create_schema,
    # create_volume
)

# Data I/O Operations
from .io import (
    # save_to_volume,
    # batch_load_with_copy_into,
    save_datamodel_to_volume,
    batch_load_datamodel_from_volume,
    get_bronze_table_name,
    # write_stream_to_delta
)

# Workspace Integration
from .workspace import get_workspace_schema_url, get_workspace_info

# Define what gets exported when using "from core import *"
__all__ = [
    # Data Models
    'Dataset',
    'DataModel',
    
    # Spark Session
    'get_spark',
    
    # Logging
    'setup_logging',
    'get_logger',
    
    # CLI
    'parse_demo_args',
    'get_demo_args_with_config',
    
    # Catalog Management
    'ensure_catalog_schema_volume',
    # 'catalog_exists',
    # 'schema_exists',
    # 'volume_exists',
    # 'create_catalog',
    # 'create_schema',
    # 'create_volume',
    
    # Data I/O
    # 'save_to_volume',
    # 'batch_load_with_copy_into',
    'save_datamodel_to_volume',
    'batch_load_datamodel_from_volume',
    'get_bronze_table_name',
    # 'write_stream_to_delta',
    
    # Workspace
    'get_workspace_schema_url',
    'get_workspace_info',
]