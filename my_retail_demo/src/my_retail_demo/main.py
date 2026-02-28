"""my_retail_demo Pipeline Orchestrator

Run:
    python -m my_retail_demo
    python -m my_retail_demo --schema my_schema --catalog my_catalog --records 500
"""

import sys

from .core import (
    get_config,
    get_spark,
    setup_logging,
    get_logger,
    parse_args,
    ensure_catalog_schema_volume,
    save_datamodel_to_volume,
    batch_load_datamodel_from_volume,
    get_workspace_schema_url,
)
from .datasets import generate_datamodel


def main():
    cli_overrides = parse_args("my_retail_demo Pipeline")
    config = get_config(cli_overrides=cli_overrides)

    setup_logging(level=config.log_level, include_timestamp=True, include_module=True)
    logger = get_logger(__name__)

    try:
        logger.info("Starting my_retail_demo pipeline...")
        if cli_overrides:
            logger.info(f"CLI overrides: {cli_overrides}")

        spark = get_spark()
        logger.info("Spark session initialized")

        ready = ensure_catalog_schema_volume(
            spark=spark,
            catalog_name=config.catalog,
            schema_name=config.schema,
            volume_name=config.volume,
            auto_create_schema=config.auto_create_schema,
            auto_create_volume=config.auto_create_volume,
        )
        if not ready:
            logger.error("Failed to ensure catalog/schema/volume")
            sys.exit(1)

        logger.info("Generating synthetic datasets...")
        num_records = cli_overrides.get("records") if cli_overrides else None
        data_model = generate_datamodel(config, num_records)

        logger.info(f"Generated {len(data_model.datasets)} datasets:")
        for ds in data_model.datasets:
            logger.info(f"  - {ds.name}: {len(ds.data):,} records")

        logger.info("Saving datasets to volume...")
        saved_paths = save_datamodel_to_volume(
            spark=spark, data_model=data_model, config=config, base_subdirectory="raw"
        )
        for path in saved_paths:
            logger.info(f"  - {path}")

        logger.info("Loading datasets to Delta tables...")
        batch_load_datamodel_from_volume(
            spark=spark,
            data_model=data_model,
            config=config,
            source_subdirectory="raw",
            drop_tables_if_exist=True,
        )

        logger.info("Pipeline completed successfully!")

        try:
            workspace_url = get_workspace_schema_url(config)
            logger.info(f"Data available at: {workspace_url}")
        except Exception as url_error:
            logger.warning(f"Could not generate workspace URL: {url_error}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        logger.exception("Full error details:")
        sys.exit(1)


if __name__ == "__main__":
    main()
