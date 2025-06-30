#!/usr/bin/env python3
"""
Schema Cleanup Script for Databricks Development

This script provides a safe way to drop schemas and all their contents (tables, volumes)
in Databricks environments. Useful for cleaning up development/demo data.

Usage:
    python scripts/drop_schema.py                           # Interactive mode
    python scripts/drop_schema.py --schema my_schema        # Drop specific schema
    python scripts/drop_schema.py --schema schema1 --schema schema2  # Drop multiple schemas
    python scripts/drop_schema.py --dry-run                 # Preview only
    python scripts/drop_schema.py --config-schema           # Use schema from config
    python scripts/drop_schema.py --help                    # Show this help

Safety Features:
    - Interactive confirmation before dropping
    - Dry-run mode to preview actions
    - Automatic exclusion of system schemas
    - Validation of schema existence before dropping
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Add src to path for imports (from scripts directory)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from config import get_config, Config
    from core.spark import get_spark
    from core.logging_config import setup_logging, get_logger
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("   Make sure you're running from the repository root and dependencies are installed")
    sys.exit(1)


class SchemaDropper:
    """Handles safe dropping of Databricks schemas with all contents."""
    
    # System schemas that should never be dropped
    PROTECTED_SCHEMAS = {
        'default', 'information_schema', 'system', 'hive_metastore',
        'samples', 'main', 'shared'
    }
    
    def __init__(self, config: Config):
        self.config = config
        self.spark = None
        self.logger = get_logger(__name__)
        
    def initialize_spark(self) -> bool:
        """Initialize Spark session."""
        try:
            self.spark = get_spark()
            self.logger.info("✅ Spark session initialized")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Spark: {e}")
            return False
    
    def validate_schema_name(self, schema_name: str) -> bool:
        """Validate that schema name is safe to drop."""
        if not schema_name or not schema_name.strip():
            self.logger.error("❌ Schema name cannot be empty")
            return False
            
        schema_name = schema_name.lower().strip()
        
        if schema_name in self.PROTECTED_SCHEMAS:
            self.logger.error(f"❌ Cannot drop protected schema: {schema_name}")
            self.logger.error(f"   Protected schemas: {', '.join(sorted(self.PROTECTED_SCHEMAS))}")
            return False
            
        return True
    
    def schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists."""
        try:
            catalog_name = self.config.databricks.catalog
            full_schema_name = f"{catalog_name}.{schema_name}"
            
            # Try to describe the schema
            result = self.spark.sql(f"DESCRIBE SCHEMA {full_schema_name}")
            return result.count() > 0
            
        except Exception as e:
            # Schema doesn't exist or other error
            self.logger.debug(f"Schema check failed: {e}")
            return False
    
    def get_schema_contents(self, schema_name: str) -> dict:
        """Get summary of schema contents (tables, volumes)."""
        try:
            catalog_name = self.config.databricks.catalog
            full_schema_name = f"{catalog_name}.{schema_name}"
            
            # Get tables
            tables_df = self.spark.sql(f"SHOW TABLES IN {full_schema_name}")
            tables = [row.tableName for row in tables_df.collect()]
            
            # Get volumes (may not be available in all Databricks versions)
            volumes = []
            try:
                volumes_df = self.spark.sql(f"SHOW VOLUMES IN {full_schema_name}")
                volumes = [row.volume_name for row in volumes_df.collect()]
            except Exception:
                self.logger.debug("SHOW VOLUMES not available or no volumes found")
            
            return {
                'tables': tables,
                'volumes': volumes,
                'total_objects': len(tables) + len(volumes)
            }
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve schema contents: {e}")
            return {'tables': [], 'volumes': [], 'total_objects': 0}
    
    def display_schema_info(self, schema_name: str, contents: dict) -> None:
        """Display schema information before dropping."""
        catalog_name = self.config.databricks.catalog
        full_schema_name = f"{catalog_name}.{schema_name}"
        
        print(f"\n📋 SCHEMA INFORMATION")
        print(f"    Full name: {full_schema_name}")
        print(f"    Tables: {len(contents['tables'])}")
        print(f"    Volumes: {len(contents['volumes'])}")
        print(f"    Total objects: {contents['total_objects']}")
        
        if contents['tables']:
            print(f"\n📊 Tables to be dropped:")
            for table in sorted(contents['tables']):
                print(f"    • {table}")
        
        if contents['volumes']:
            print(f"\n💾 Volumes to be dropped:")
            for volume in sorted(contents['volumes']):
                print(f"    • {volume}")
    
    def confirm_drop(self, schema_name: str, contents: dict) -> bool:
        """Get user confirmation before dropping schema."""
        print(f"\n⚠️  WARNING: This will permanently delete the schema and ALL its contents!")
        print(f"   This action cannot be undone.")
        
        if contents['total_objects'] == 0:
            response = input(f"\nDrop empty schema '{schema_name}'? (yes/no): ")
        else:
            response = input(f"\nDrop schema '{schema_name}' with {contents['total_objects']} objects? (yes/no): ")
        
        return response.lower() in ['yes', 'y']
    
    def confirm_multiple_drops(self, schema_names: list, all_contents: dict, total_objects: int) -> bool:
        """Get user confirmation before dropping multiple schemas."""
        print(f"\n⚠️  WARNING: This will permanently delete {len(schema_names)} schema(s) and ALL their contents!")
        print(f"   This action cannot be undone.")
        print(f"\n📊 Summary:")
        print(f"   • Schemas to drop: {len(schema_names)}")
        print(f"   • Total objects: {total_objects}")
        
        print(f"\n📂 Schemas to be dropped:")
        for schema_name in schema_names:
            contents = all_contents[schema_name]
            print(f"   • {schema_name} ({contents['total_objects']} objects)")
        
        response = input(f"\nDrop all {len(schema_names)} schema(s) with {total_objects} total objects? (yes/no): ")
        return response.lower() in ['yes', 'y']
    
    def drop_schema(self, schema_name: str, dry_run: bool = False) -> bool:
        """Drop the specified schema with CASCADE."""
        try:
            catalog_name = self.config.databricks.catalog
            full_schema_name = f"{catalog_name}.{schema_name}"
            
            drop_sql = f"DROP SCHEMA IF EXISTS {full_schema_name} CASCADE"
            
            if dry_run:
                print(f"\n🔍 DRY RUN - Would execute: {drop_sql}")
                return True
            
            self.logger.info(f"Executing: {drop_sql}")
            self.spark.sql(drop_sql)
            
            print(f"✅ Successfully dropped schema: {full_schema_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to drop schema: {e}")
            return False
    
    def interactive_schema_selection(self) -> Optional[list]:
        """Interactive schema selection supporting multiple schemas."""
        try:
            # List available schemas in the catalog
            catalog_name = self.config.databricks.catalog
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
            
            # Get the first column (schema name) regardless of column name
            schemas_rows = schemas_df.collect()
            if not schemas_rows:
                print("❌ No schemas found in catalog")
                return None
            
            # Use the first column for schema names (could be 'databaseName', 'schemaName', 'namespace')
            first_column = schemas_df.columns[0]
            schemas = [getattr(row, first_column) for row in schemas_rows]
            
            # Filter out protected schemas
            available_schemas = [s for s in schemas if s.lower() not in self.PROTECTED_SCHEMAS]
            
            if not available_schemas:
                print("❌ No schemas available to drop")
                return None
            
            print(f"\n📂 Available schemas in catalog '{catalog_name}':")
            for i, schema in enumerate(available_schemas, 1):
                print(f"   {i}. {schema}")
            
            print(f"\n💡 Instructions:")
            print(f"   • Single schema: Enter number (e.g., '1')")
            print(f"   • Multiple schemas: Enter numbers separated by commas (e.g., '1,3,5')")
            print(f"   • Range: Enter range with dash (e.g., '1-3' for schemas 1, 2, 3)")
            print(f"   • All schemas: Enter 'all'")
            print(f"   • Quit: Enter 'q'")
            
            while True:
                try:
                    choice = input(f"\nSelect schema(s) to drop: ").strip()
                    if choice.lower() == 'q':
                        return None
                    
                    if choice.lower() == 'all':
                        return available_schemas.copy()
                    
                    selected_schemas = []
                    
                    # Handle comma-separated choices
                    if ',' in choice:
                        parts = [p.strip() for p in choice.split(',')]
                        for part in parts:
                            if '-' in part:
                                # Handle range
                                start, end = map(int, part.split('-'))
                                for i in range(start, end + 1):
                                    if 1 <= i <= len(available_schemas):
                                        schema = available_schemas[i - 1]
                                        if schema not in selected_schemas:
                                            selected_schemas.append(schema)
                            else:
                                # Handle single number
                                index = int(part) - 1
                                if 0 <= index < len(available_schemas):
                                    schema = available_schemas[index]
                                    if schema not in selected_schemas:
                                        selected_schemas.append(schema)
                    elif '-' in choice:
                        # Handle range
                        start, end = map(int, choice.split('-'))
                        for i in range(start, end + 1):
                            if 1 <= i <= len(available_schemas):
                                schema = available_schemas[i - 1]
                                if schema not in selected_schemas:
                                    selected_schemas.append(schema)
                    else:
                        # Handle single choice
                        index = int(choice) - 1
                        if 0 <= index < len(available_schemas):
                            selected_schemas.append(available_schemas[index])
                    
                    if selected_schemas:
                        return selected_schemas
                    else:
                        print(f"❌ Invalid selection. Please try again.")
                        
                except ValueError:
                    print("❌ Invalid input. Please enter numbers, ranges, or 'q'")
                    
        except Exception as e:
            self.logger.error(f"Failed to list schemas: {e}")
            return None
    
    def run(self, schema_names: list = None, dry_run: bool = False, 
            use_config_schema: bool = False) -> bool:
        """Main execution logic."""
        print("🗑️  Schema Cleanup Tool")
        print(f"📂 Catalog: {self.config.databricks.catalog}")
        print()
        
        if not self.initialize_spark():
            return False
        
        # Determine target schemas
        if use_config_schema:
            schema_names = [self.config.databricks.schema]
            print(f"🎯 Using schema from config: {schema_names[0]}")
        elif not schema_names:
            schema_names = self.interactive_schema_selection()
            if not schema_names:
                print("👋 Operation cancelled")
                return True
        
        # Ensure schema_names is a list
        if isinstance(schema_names, str):
            schema_names = [schema_names]
        
        # Validate all schema names
        valid_schemas = []
        for schema_name in schema_names:
            if not self.validate_schema_name(schema_name):
                continue
            
            # Check if schema exists
            if not self.schema_exists(schema_name):
                catalog_name = self.config.databricks.catalog
                print(f"❌ Schema '{catalog_name}.{schema_name}' does not exist")
                continue
            
            valid_schemas.append(schema_name)
        
        if not valid_schemas:
            print("❌ No valid schemas to drop")
            return False
        
        # Display information for all schemas
        all_contents = {}
        total_objects = 0
        
        for schema_name in valid_schemas:
            contents = self.get_schema_contents(schema_name)
            all_contents[schema_name] = contents
            total_objects += contents['total_objects']
            self.display_schema_info(schema_name, contents)
        
        # Get confirmation for all schemas (unless dry run)
        if not dry_run:
            if not self.confirm_multiple_drops(valid_schemas, all_contents, total_objects):
                print("👋 Operation cancelled")
                return True
        
        # Execute drops
        success_count = 0
        for schema_name in valid_schemas:
            if self.drop_schema(schema_name, dry_run):
                success_count += 1
            else:
                print(f"❌ Failed to drop schema: {schema_name}")
        
        # Summary
        if not dry_run:
            print(f"\n📊 Summary: {success_count}/{len(valid_schemas)} schemas dropped successfully")
        
        return success_count == len(valid_schemas)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Drop Databricks schemas with all contents (tables, volumes)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--schema',
        action='append',
        help='Schema name to drop (can be used multiple times for multiple schemas)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be dropped without making changes'
    )
    parser.add_argument(
        '--config-schema',
        action='store_true',
        help='Use schema from configuration file'
    )
    
    args = parser.parse_args()
    
    # Load configuration first
    config = get_config()
    
    # Setup logging using configured level
    setup_logging(level=config.logging.level, include_timestamp=True, include_module=True)
    logger = get_logger(__name__)
    
    try:
        
        # Create and run dropper
        dropper = SchemaDropper(config)
        success = dropper.run(
            schema_names=args.schema,
            dry_run=args.dry_run,
            use_config_schema=args.config_schema
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()