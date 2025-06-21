"""Sales Demo Pipeline Orchestrator

Main execution pipeline for the sales demo. This demo is a reference implementation of the sales demo.

Execution:
    python -m reference_demos.sales_demo.main
    python -m reference_demos.sales_demo.main --schema my_custom_schema
"""

import sys

from config.settings import get_config
from core.cli import parse_demo_args
from core.spark import get_spark
from core.catalog import ensure_catalog_schema_volume
from core.io import save_datamodel_to_volume, batch_load_datamodel_from_volume
from core.logging_config import setup_logging, get_logger
from core.workspace import get_workspace_schema_url

from .datasets import data_model

def main():
    # Parse command line arguments using centralized parsing
    cli_overrides = parse_demo_args("Sales Demo Pipeline")
    
    # Setup centralized logging first
    setup_logging(level="INFO", include_timestamp=True, include_module=True)
    logger = get_logger(__name__)
    
    logger.info("Starting sales demo pipeline")
    
    try:
        if cli_overrides:
            logger.info(f"CLI overrides provided: {cli_overrides}")
        
        # Load configuration with CLI overrides
        config = get_config(cli_overrides=cli_overrides)
        logger.info(f"Loaded configuration for environment: {config.app.name}")
        
        # Initialize Spark session
        spark = get_spark()
        logger.info("Spark session initialized")
        
        # Ensure catalog, schema, and volume exist
        ready = ensure_catalog_schema_volume(
            spark=spark,
            catalog_name=config.databricks.catalog,
            schema_name=config.databricks.schema,
            volume_name=config.databricks.volume,
            auto_create_catalog=config.databricks.auto_create_catalog,
            auto_create_schema=config.databricks.auto_create_schema,
            auto_create_volume=config.databricks.auto_create_volume
        )
        
        if not ready:
            logger.error("Failed to ensure catalog/schema/volume are ready")
            sys.exit(1)
        
        logger.info("Catalog, schema, and volume are ready")
        
        # Save datasets to volume (Bronze layer)
        logger.info("Saving datasets to volume...")
        saved_paths = save_datamodel_to_volume(
            spark=spark,
            data_model=data_model,
            config=config,
            base_subdirectory="raw"
        )
        
        logger.info(f"Saved {len(saved_paths)} datasets to volume")
        for path in saved_paths:
            logger.info(f"  - {path}")
        
        # Load datasets from volume to Delta tables
        logger.info("Loading datasets to Delta tables...")
        loaded_dfs = batch_load_datamodel_from_volume(
            spark=spark,
            data_model=data_model,
            config=config,
            source_subdirectory="raw",
            target_schema_suffix="_bronze",
            drop_tables_if_exist=True
        )
        
        logger.info(f"Loaded {len(loaded_dfs)} datasets to Delta tables")
        
        # Display sample data
        for i, df in enumerate(loaded_dfs):
            dataset_name = data_model.datasets[i].name
            logger.info(f"Sample data from {dataset_name}:")
            df.show(5, truncate=False)
            logger.info(f"Total records in {dataset_name}: {df.count()}")
        
        logger.info("Sales demo pipeline completed successfully")
        
        # Generate and display workspace URL
        try:
            workspace_url = get_workspace_schema_url(config)
            logger.info(f"Data available at: {workspace_url}")
        except Exception as url_error:
            logger.warning(f"Could not generate workspace URL: {str(url_error)}")
        
    except Exception as e:
        logger.error(f"Sales demo failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()