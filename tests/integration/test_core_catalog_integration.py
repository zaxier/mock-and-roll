"""
Integration tests for core.catalog module.
Tests actual catalog, schema, and volume operations with real Spark session.
"""

import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession

from core.catalog import ensure_catalog_schema_volume


@pytest.mark.integration
@pytest.mark.spark
class TestCatalogIntegrationWorkflows:
    """Integration tests for complete catalog workflow scenarios."""
    
    def test_ensure_catalog_schema_volume_workflow(self, spark_session, test_config):
        """Test the complete ensure_catalog_schema_volume workflow."""
        catalog_name = test_config.databricks.catalog
        schema_name = test_config.databricks.schema
        volume_name = test_config.databricks.volume
        
        # Test with auto-creation disabled (should handle gracefully in local mode)
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,
            auto_create_catalog=False,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        # In local mode, this will likely return False due to Unity Catalog not being available
        # But the function should handle this gracefully without crashing
        assert isinstance(result, bool)
    
    def test_ensure_catalog_schema_volume_with_auto_creation(self, spark_session, test_config):
        """Test ensure_catalog_schema_volume with auto-creation enabled."""
        catalog_name = test_config.databricks.catalog
        schema_name = test_config.databricks.schema
        volume_name = test_config.databricks.volume
        
        # Test with auto-creation enabled
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,
            auto_create_catalog=True,
            auto_create_schema=True,
            auto_create_volume=True
        )
        
        # In local mode, creation attempts will likely fail but should be handled gracefully
        assert isinstance(result, bool)
    
    def test_catalog_operations_error_handling(self, spark_session, test_config):
        """Test error handling in catalog operations."""
        # Test with invalid characters in names
        invalid_catalog = "invalid-catalog-name!"
        invalid_schema = "invalid schema name"
        invalid_volume = "invalid/volume/name"
        
        # These should handle errors gracefully
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=invalid_catalog,
            schema_name=invalid_schema,
            volume_name=invalid_volume,
            auto_create_catalog=False,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        # Should return False for invalid names
        assert result is False
    
    def test_catalog_operations_with_none_values(self, spark_session):
        """Test catalog operations with None values."""
        # Test with None values - should handle gracefully
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=None,
            schema_name=None,
            volume_name=None,
            auto_create_catalog=False,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        # Should return False for None values
        assert result is False


@pytest.mark.integration
@pytest.mark.spark
class TestCatalogIntegrationWithMocking:
    """Integration tests that combine real Spark with strategic mocking for Databricks-specific features."""
    
    @patch('core.catalog.catalog_exists')
    @patch('core.catalog.schema_exists')
    @patch('core.catalog.volume_exists')
    def test_successful_resource_verification(self, mock_volume_exists, mock_schema_exists, mock_catalog_exists, spark_session, test_config):
        """Test successful resource verification workflow."""
        # Mock successful existence checks
        mock_catalog_exists.return_value = True
        mock_schema_exists.return_value = True
        mock_volume_exists.return_value = True
        
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=test_config.databricks.catalog,
            schema_name=test_config.databricks.schema,
            volume_name=test_config.databricks.volume,
            auto_create_catalog=False,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        assert result is True
        mock_catalog_exists.assert_called_once_with(spark_session, test_config.databricks.catalog)
        mock_schema_exists.assert_called_once_with(spark_session, test_config.databricks.catalog, test_config.databricks.schema)
        mock_volume_exists.assert_called_once_with(spark_session, test_config.databricks.catalog, test_config.databricks.schema, test_config.databricks.volume)
    
    @patch('core.catalog.catalog_exists')
    @patch('core.catalog.schema_exists') 
    @patch('core.catalog.volume_exists')
    def test_missing_resources_no_auto_create(self, mock_volume_exists, mock_schema_exists, mock_catalog_exists, spark_session, test_config):
        """Test behavior when resources don't exist and auto-creation is disabled."""
        # Mock resources don't exist
        mock_catalog_exists.return_value = False
        mock_schema_exists.return_value = False
        mock_volume_exists.return_value = False
        
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=test_config.databricks.catalog,
            schema_name=test_config.databricks.schema,
            volume_name=test_config.databricks.volume,
            auto_create_catalog=False,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        assert result is False
        mock_catalog_exists.assert_called_once_with(spark_session, test_config.databricks.catalog)
    
    @patch('core.catalog.catalog_exists')
    @patch('core.catalog.create_catalog')
    @patch('core.catalog.schema_exists')
    @patch('core.catalog.create_schema')
    @patch('core.catalog.volume_exists')
    @patch('core.catalog.create_volume')
    def test_auto_creation_workflow(self, mock_create_volume, mock_volume_exists, mock_create_schema, 
                                  mock_schema_exists, mock_create_catalog, mock_catalog_exists, 
                                  spark_session, test_config):
        """Test auto-creation workflow when resources don't exist."""
        # Mock initial checks - resources don't exist
        mock_catalog_exists.return_value = False
        mock_schema_exists.return_value = False
        mock_volume_exists.return_value = False
        
        # Mock successful creation
        mock_create_catalog.return_value = True
        mock_create_schema.return_value = True
        mock_create_volume.return_value = True
        
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=test_config.databricks.catalog,
            schema_name=test_config.databricks.schema,
            volume_name=test_config.databricks.volume,
            auto_create_catalog=True,
            auto_create_schema=True,
            auto_create_volume=True
        )
        
        assert result is True
        
        # Verify creation attempts
        mock_create_catalog.assert_called_once_with(spark_session, test_config.databricks.catalog, f"Auto-created catalog for demo purposes")
        mock_create_schema.assert_called_once_with(spark_session, test_config.databricks.catalog, test_config.databricks.schema, f"Auto-created schema for demo purposes")
        mock_create_volume.assert_called_once_with(spark_session, test_config.databricks.catalog, test_config.databricks.schema, test_config.databricks.volume, f"Auto-created volume for demo data")


@pytest.mark.integration
@pytest.mark.spark
class TestCatalogIntegrationLogging:
    """Integration tests focused on logging behavior."""
    
    @patch('core.catalog.logger')
    def test_catalog_operations_logging(self, mock_logger, spark_session, test_config):
        """Test that catalog operations produce appropriate log messages."""
        # Run catalog operations
        result = ensure_catalog_schema_volume(
            spark=spark_session,
            catalog_name=test_config.databricks.catalog,
            schema_name=test_config.databricks.schema,
            volume_name=test_config.databricks.volume,
            auto_create_catalog=True,
            auto_create_schema=True,
            auto_create_volume=True
        )
        
        # Verify logging occurred
        assert mock_logger.info.called or mock_logger.warning.called or mock_logger.error.called


@pytest.mark.integration
@pytest.mark.spark
class TestCatalogIntegrationPerformance:
    """Integration tests focused on performance and resource usage."""
    
    def test_catalog_operations_performance(self, spark_session, test_config):
        """Test performance of catalog operations."""
        import time
        
        start_time = time.time()
        
        # Run multiple catalog operations
        for i in range(3):
            catalog_name = f"{test_config.databricks.catalog}_{i}"
            schema_name = f"{test_config.databricks.schema}_{i}"
            volume_name = f"{test_config.databricks.volume}_{i}"
            
            result = ensure_catalog_schema_volume(
                spark=spark_session,
                catalog_name=catalog_name,
                schema_name=schema_name,
                volume_name=volume_name,
                auto_create_catalog=False,
                auto_create_schema=False,
                auto_create_volume=False
            )
            
            assert isinstance(result, bool)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Should complete within reasonable time (adjust threshold as needed)
        assert execution_time < 30.0, f"Catalog operations took too long: {execution_time} seconds"
    
    def test_catalog_operations_memory_usage(self, spark_session, test_config):
        """Test memory usage of catalog operations."""
        import gc
        import sys
        
        # Force garbage collection before measurement
        gc.collect()
        initial_objects = len(gc.get_objects())
        
        # Run catalog operations
        for i in range(5):
            result = ensure_catalog_schema_volume(
                spark=spark_session,
                catalog_name=f"test_catalog_{i}",
                schema_name=f"test_schema_{i}",
                volume_name=f"test_volume_{i}",
                auto_create_catalog=False,
                auto_create_schema=False,
                auto_create_volume=False
            )
        
        # Force garbage collection after operations
        gc.collect()
        final_objects = len(gc.get_objects())
        
        # Check that we haven't created excessive objects (adjust threshold as needed)
        object_increase = final_objects - initial_objects
        assert object_increase < 1000, f"Too many objects created: {object_increase}"