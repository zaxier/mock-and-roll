"""Sales Demo Pipeline Orchestrator

Main execution pipeline for the sales demo. This demo is a reference implementation of the sales demo.

Execution:
    python -m examples.sales_demo.main
    python -m examples.sales_demo.main --schema my_custom_schema
"""

import sys

from config import get_config
from core import *

from .datasets import generate_datamodel

def main():
    # Parse command line arguments using centralized parsing
    cli_overrides = parse_demo_args("Sales Demo Pipeline")
    
    # Load configuration with CLI overrides
    config = get_config(cli_overrides=cli_overrides)
    
    # Setup centralized logging using configured level
    setup_logging(level=config.logging.level, include_timestamp=True, include_module=True)
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting sales demo pipeline...")
        logger.info("This demo creates user profiles and sales transactions with proper relationships")
        
        if cli_overrides:
            logger.info(f"CLI overrides provided: {cli_overrides}")
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
        
        # Generate synthetic datasets
        logger.info("Generating synthetic sales datasets...")
        num_records = cli_overrides.get('records') if cli_overrides else None
        data_model = generate_datamodel(config, num_records)
        
        # Log dataset statistics
        logger.info(f"Generated {len(data_model.datasets)} datasets:")
        for dataset in data_model.datasets:
            logger.info(f"  - {dataset.name}: {len(dataset.data):,} records")
        
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
            drop_tables_if_exist=True
        )
        
        logger.info(f"Loaded {len(loaded_dfs)} datasets to Delta tables")
        
        # Create Silver layer aggregations
        logger.info("Creating business-ready aggregations (Silver layer)...")
        
        # Daily sales summary
        product_sales_bronze = get_bronze_table_name("product_sales")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.daily_sales_silver AS
            SELECT 
                DATE(sale_date) as date,
                COUNT(DISTINCT transaction_id) as num_transactions,
                COUNT(DISTINCT user_id) as unique_customers,
                SUM(amount) as total_sales,
                AVG(amount) as avg_transaction_value,
                SUM(quantity) as units_sold
            FROM {config.databricks.catalog}.{config.databricks.schema}.{product_sales_bronze}
            GROUP BY DATE(sale_date)
            ORDER BY date DESC
        """)
        
        # Customer lifetime value
        user_profiles_bronze = get_bronze_table_name("user_profiles")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.customer_lifetime_value_silver AS
            SELECT 
                c.user_id,
                c.full_name,
                c.email,
                c.customer_type,
                c.signup_date,
                COUNT(DISTINCT s.transaction_id) as total_transactions,
                COALESCE(SUM(s.amount), 0) as lifetime_value,
                COALESCE(AVG(s.amount), 0) as avg_order_value,
                MAX(s.sale_date) as last_purchase_date,
                DATEDIFF(CURRENT_DATE(), c.signup_date) as days_since_signup
            FROM {config.databricks.catalog}.{config.databricks.schema}.{user_profiles_bronze} c
            LEFT JOIN {config.databricks.catalog}.{config.databricks.schema}.{product_sales_bronze} s
                ON c.user_id = s.user_id
            GROUP BY c.user_id, c.full_name, c.email, c.customer_type, c.signup_date
        """)
        
        # Product performance
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.product_performance_silver AS
            SELECT 
                product,
                COUNT(DISTINCT transaction_id) as num_transactions,
                SUM(quantity) as units_sold,
                SUM(amount) as revenue,
                AVG(unit_price) as avg_price,
                COUNT(DISTINCT user_id) as unique_customers
            FROM {config.databricks.catalog}.{config.databricks.schema}.{product_sales_bronze}
            GROUP BY product
            ORDER BY revenue DESC
        """)
        
        # Customer segments
        spark.sql(f"""
            CREATE OR REPLACE TABLE {config.databricks.catalog}.{config.databricks.schema}.customer_segments_silver AS
            WITH customer_metrics AS (
                SELECT 
                    user_id,
                    COUNT(DISTINCT transaction_id) as transaction_count,
                    SUM(amount) as total_spent,
                    MAX(sale_date) as last_transaction_date,
                    DATEDIFF(CURRENT_DATE(), MAX(sale_date)) as days_since_last_purchase
                FROM {config.databricks.catalog}.{config.databricks.schema}.{product_sales_bronze}
                GROUP BY user_id
            )
            SELECT 
                c.user_id,
                c.customer_type,
                COALESCE(m.transaction_count, 0) as transaction_count,
                COALESCE(m.total_spent, 0) as total_spent,
                CASE 
                    WHEN m.total_spent > 5000 THEN 'High Value'
                    WHEN m.total_spent > 1000 THEN 'Medium Value'
                    WHEN m.total_spent > 0 THEN 'Low Value'
                    ELSE 'No Purchases'
                END as value_segment,
                CASE
                    WHEN m.days_since_last_purchase <= 30 THEN 'Active'
                    WHEN m.days_since_last_purchase <= 90 THEN 'At Risk'
                    WHEN m.days_since_last_purchase <= 180 THEN 'Dormant'
                    WHEN m.days_since_last_purchase > 180 THEN 'Lost'
                    ELSE 'Never Purchased'
                END as activity_segment
            FROM {config.databricks.catalog}.{config.databricks.schema}.{user_profiles_bronze} c
            LEFT JOIN customer_metrics m ON c.user_id = m.user_id
        """)
        
        logger.info("Silver layer tables created successfully")
        
        # Log summary statistics
        logger.info("\n=== Pipeline Summary ===")
        logger.info(f"Total Users: {len(data_model.get_dataset('user_profiles').data):,}")
        logger.info(f"Total Transactions: {len(data_model.get_dataset('product_sales').data):,}")
        
        logger.info("\nSales demo pipeline completed successfully!")
        
        # Generate and display workspace URL for easy access
        try:
            workspace_url = get_workspace_schema_url(config)
            logger.info(f"\n🎯 Data available at: {workspace_url}")
            logger.info("\nKey tables to explore:")
            logger.info(f"  - {user_profiles_bronze}: Customer dimension table")
            logger.info(f"  - {product_sales_bronze}: Sales fact table")
            logger.info("  - daily_sales_silver: Daily sales summaries")
            logger.info("  - customer_lifetime_value_silver: Customer value analysis")
            logger.info("  - product_performance_silver: Product sales metrics")
            logger.info("  - customer_segments_silver: Customer segmentation")
        except Exception as url_error:
            logger.warning(f"Could not generate workspace URL: {str(url_error)}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.exception("Full error details:")
        sys.exit(1)

if __name__ == "__main__":
    main()