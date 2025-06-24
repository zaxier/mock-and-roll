"""
Sporting Goods Retailer Demo Pipeline

This pipeline generates synthetic data for a sporting goods retailer with:
- Omnichannel retail operations (physical stores + online)
- Supply chain optimization capabilities
- Customer behavior tracking across channels
- Inventory management across stores and warehouses
"""

import sys
from datetime import datetime

from core.cli import parse_demo_args
from core.logging_config import setup_logging, get_logger
from core.workspace import get_workspace_schema_url
from config import get_config
from core.spark import get_spark
from core.catalog import ensure_catalog_schema_volume
from core.io import save_datamodel_to_volume, batch_load_datamodel_from_volume

from .datasets import generate_datasets


def main():
    """Main orchestrator for the sporting goods retailer demo pipeline."""
    # Parse command line arguments using centralized parsing
    cli_overrides = parse_demo_args("Sporting Goods Retailer Omnichannel Demo Pipeline")
    
    # Setup centralized logging first
    setup_logging(level="INFO")
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting Sporting Goods Retailer demo pipeline...")
        logger.info("This demo creates an omnichannel retail dataset with supply chain data")
        
        if cli_overrides:
            logger.info(f"CLI overrides provided: {cli_overrides}")
        
        # Load configuration with CLI overrides
        config = get_config(cli_overrides=cli_overrides)
        
        # Initialize Spark session
        logger.info("Initializing Spark session...")
        spark = get_spark()
        
        # Ensure catalog, schema, and volume exist
        logger.info(f"Ensuring catalog/schema/volume exist: {config.databricks.catalog}.{config.databricks.schema}.{config.databricks.volume}")
        catalog_ready = ensure_catalog_schema_volume(
            spark=spark,
            catalog_name=config.databricks.catalog,
            schema_name=config.databricks.schema,
            volume_name=config.databricks.volume,
            auto_create_catalog=config.databricks.auto_create_catalog,
            auto_create_schema=config.databricks.auto_create_schema,
            auto_create_volume=config.databricks.auto_create_volume
        )
        
        if not catalog_ready:
            raise Exception("Failed to ensure catalog/schema/volume setup")
        
        # Generate synthetic datasets
        logger.info("Generating synthetic sporting goods retailer datasets...")
        num_records = cli_overrides.get('records') if cli_overrides else None
        data_model = generate_datasets(config, num_records)
        
        # Log dataset statistics
        logger.info(f"Generated {len(data_model.datasets)} datasets:")
        for dataset in data_model.datasets:
            logger.info(f"  - {dataset.name}: {len(dataset.data):,} records")
        
        # Save datasets to volume
        logger.info("Saving datasets to Databricks Volume...")
        saved_paths = save_datamodel_to_volume(
            spark=spark,
            data_model=data_model,
            config=config,
            base_subdirectory="raw"
        )
        
        logger.info(f"Saved {len(saved_paths)} datasets to volume")
        
        # Load datasets to Delta tables (Bronze layer)
        logger.info("Loading datasets to Delta tables (Bronze layer)...")
        loaded_dfs = batch_load_datamodel_from_volume(
            spark=spark,
            data_model=data_model,
            config=config,
            source_subdirectory="raw",
            target_schema_suffix="_bronze",
            drop_tables_if_exist=True
        )
        
        logger.info(f"Successfully loaded {len(loaded_dfs)} datasets to Delta tables")
        
        # Create sample aggregations (Silver layer)
        logger.info("Creating business-ready aggregations (Silver layer)...")
        
        # Daily sales summary
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.daily_sales_silver AS
            SELECT 
                DATE(transaction_date) as sale_date,
                channel,
                COUNT(DISTINCT transaction_id) as num_transactions,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(CASE WHEN NOT is_return THEN line_total ELSE 0 END) as gross_sales,
                SUM(CASE WHEN is_return THEN ABS(line_total) ELSE 0 END) as returns,
                SUM(line_total) as net_sales,
                SUM(loyalty_points_earned) as total_loyalty_points
            FROM {config.databricks.catalog}.{config.databricks.schema}.transactions_bronze
            GROUP BY DATE(transaction_date), channel
        """)
        
        # Product performance
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.product_performance_silver AS
            SELECT 
                p.product_id,
                p.product_name,
                p.brand,
                p.category,
                p.subcategory,
                COUNT(DISTINCT t.transaction_id) as num_transactions,
                SUM(t.quantity) as units_sold,
                SUM(t.line_total) as revenue,
                AVG(t.discount_percent) as avg_discount
            FROM {config.databricks.catalog}.{config.databricks.schema}.products_bronze p
            LEFT JOIN {config.databricks.catalog}.{config.databricks.schema}.transactions_bronze t
                ON p.product_id = t.product_id
            WHERE t.is_return = false
            GROUP BY p.product_id, p.product_name, p.brand, p.category, p.subcategory
        """)
        
        # Inventory optimization view
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.inventory_optimization_silver AS
            SELECT 
                i.location_id,
                i.location_type,
                i.product_id,
                p.product_name,
                p.category,
                i.quantity_on_hand,
                i.reorder_point,
                i.min_stock_level,
                i.max_stock_level,
                CASE 
                    WHEN i.quantity_on_hand < i.reorder_point THEN 'Reorder Required'
                    WHEN i.quantity_on_hand < i.min_stock_level THEN 'Critical Low'
                    WHEN i.quantity_on_hand > i.max_stock_level THEN 'Overstock'
                    ELSE 'Optimal'
                END as stock_status,
                i.max_stock_level - i.quantity_on_hand as suggested_order_quantity
            FROM {config.databricks.catalog}.{config.databricks.schema}.inventory_bronze i
            JOIN {config.databricks.catalog}.{config.databricks.schema}.products_bronze p
                ON i.product_id = p.product_id
        """)
        
        # Customer segmentation
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.customer_segments_silver AS
            SELECT 
                c.customer_id,
                c.customer_type,
                c.preferred_channel,
                c.lifetime_value,
                c.loyalty_points,
                COUNT(DISTINCT t.transaction_id) as total_transactions,
                SUM(t.line_total) as total_spent,
                MAX(t.transaction_date) as last_transaction_date,
                CASE 
                    WHEN c.lifetime_value > 10000 THEN 'High Value'
                    WHEN c.lifetime_value > 5000 THEN 'Medium Value'
                    WHEN c.lifetime_value > 1000 THEN 'Regular'
                    ELSE 'Low Value'
                END as value_segment
            FROM {config.databricks.catalog}.{config.databricks.schema}.customers_bronze c
            LEFT JOIN {config.databricks.catalog}.{config.databricks.schema}.transactions_bronze t
                ON c.customer_id = t.customer_id AND t.is_return = false
            GROUP BY c.customer_id, c.customer_type, c.preferred_channel, c.lifetime_value, c.loyalty_points
        """)
        
        logger.info("Silver layer tables created successfully")
        
        # Log summary statistics
        logger.info("\n=== Pipeline Summary ===")
        logger.info(f"Total Stores: {len(data_model.get_dataset('stores').data):,}")
        logger.info(f"Total Products: {len(data_model.get_dataset('products').data):,}")
        logger.info(f"Total Customers: {len(data_model.get_dataset('customers').data):,}")
        logger.info(f"Total Transactions: {len(data_model.get_dataset('transactions').data):,}")
        logger.info(f"Total Customer Events: {len(data_model.get_dataset('customer_events').data):,}")
        
        logger.info("\nPipeline completed successfully!")
        
        # Generate and display workspace URL for easy access
        try:
            workspace_url = get_workspace_schema_url(config)
            logger.info(f"\n🎯 Data available at: {workspace_url}")
            logger.info("\nKey tables to explore:")
            logger.info("  - stores_bronze: Physical store locations")
            logger.info("  - products_bronze: Product catalog")
            logger.info("  - customers_bronze: Customer profiles")
            logger.info("  - transactions_bronze: Sales transactions")
            logger.info("  - inventory_bronze: Current inventory levels")
            logger.info("  - daily_sales_silver: Daily sales summaries")
            logger.info("  - product_performance_silver: Product analytics")
            logger.info("  - inventory_optimization_silver: Stock optimization insights")
            logger.info("  - customer_segments_silver: Customer segmentation")
        except Exception as url_error:
            logger.warning(f"Could not generate workspace URL: {str(url_error)}")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.exception("Full error details:")
        sys.exit(1)


if __name__ == "__main__":
    main()