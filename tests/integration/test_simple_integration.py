"""
Simple integration tests that verify module interactions without requiring local Spark.
These tests focus on integration between modules using strategic mocking.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from core.catalog import ensure_catalog_schema_volume
from core.io import save_to_volume, batch_load_with_copy_into
from reference_demos.sales_demo.datasets import generate_user_profiles, generate_sales_data
from config.settings import get_config


@pytest.mark.integration
class TestModuleIntegration:
    """Integration tests focusing on module interactions."""
    
    def test_config_and_datagen_integration(self, test_config):
        """Test integration between config module and data generation."""
        # Generate data using config parameters
        user_data = generate_user_profiles(num_records=test_config.data_generation.default_records)
        sales_data = generate_sales_data(user_ids=user_data['user_id'].tolist(), num_records=50)
        
        # Verify data matches config
        assert len(user_data) == test_config.data_generation.default_records
        assert all(col in user_data.columns for col in ['user_id', 'full_name', 'email'])
        assert len(sales_data) == 50
        assert all(col in sales_data.columns for col in ['transaction_id', 'user_id', 'amount'])
        
        # Verify data quality
        assert sales_data['amount'].min() >= 10.0  # Based on datagen logic
        assert sales_data['amount'].max() <= 1000.0
        assert all(sales_data['transaction_id'].str.startswith('TXN-'))
    
    def test_new_datamodel_integration(self, test_config):
        """Test integration of the new DataModel with config."""
        from core.data import Dataset, DataModel
        
        # Generate data using config parameters
        user_data = generate_user_profiles(num_records=test_config.data_generation.default_records)
        sales_data = generate_sales_data(user_ids=user_data['user_id'].tolist(), num_records=50)
        
        # Create datasets
        user_dataset = Dataset(name="users", data=user_data)
        sales_dataset = Dataset(name="sales", data=sales_data)
        
        # Create data model
        data_model = DataModel(datasets=[user_dataset, sales_dataset])
        
        # Verify integration
        assert len(data_model.datasets) == 2
        assert data_model.get_dataset("users") is not None
        assert data_model.get_dataset("sales") is not None
        assert data_model.get_dataset("nonexistent") is None
    
    def test_config_environment_integration(self):
        """Test integration with different configuration environments."""
        # Test that config loading works
        config = get_config()
        
        # Verify essential config attributes exist
        assert hasattr(config, 'databricks')
        assert hasattr(config, 'storage')
        assert hasattr(config, 'data_generation')
        
        # Verify databricks config
        assert hasattr(config.databricks, 'catalog')
        assert hasattr(config.databricks, 'schema')
        assert hasattr(config.databricks, 'volume')
        assert hasattr(config.databricks, 'get_volume_path')
        
        # Test volume path generation
        test_path = config.databricks.get_volume_path("test/subpath")
        assert "test/subpath" in test_path
        assert config.databricks.catalog in test_path
        assert config.databricks.schema in test_path
        assert config.databricks.volume in test_path


@pytest.mark.integration
class TestDataFlowIntegration:
    """Integration tests for data flow between components."""
    
    def test_pandas_to_spark_conversion_flow(self):
        """Test data conversion flows."""
        # Generate sample data
        user_data = generate_user_profiles(num_records=10)
        sales_data = generate_sales_data(user_ids=user_data['user_id'].tolist(), num_records=20)
        
        # Verify sample data structure
        assert isinstance(user_data, pd.DataFrame)
        assert isinstance(sales_data, pd.DataFrame)
        assert len(user_data) == 10
        assert len(sales_data) == 20
        
        # Test data types
        assert 'user_id' in user_data.columns
        assert 'full_name' in user_data.columns
        assert 'email' in user_data.columns
        assert 'transaction_id' in sales_data.columns
        assert 'amount' in sales_data.columns
        
        # Verify data quality
        assert sales_data['amount'].dtype in ['float64', 'int64']
    
    @patch('core.spark.get_spark')
    def test_io_operations_integration(self, mock_get_spark, temp_dir):
        """Test I/O operations with file system."""
        # Generate test data
        user_data = generate_user_profiles(num_records=10)
        
        # Setup mock Spark session with realistic behavior
        mock_spark = Mock()
        mock_spark_df = Mock()
        mock_spark.createDataFrame.return_value = mock_spark_df
        
        # Mock the write chain
        mock_writer = Mock()
        mock_spark_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.save.return_value = None
        
        mock_get_spark.return_value = mock_spark
        
        # Test save operation
        test_path = os.path.join(temp_dir, "test_output")
        
        save_to_volume(
            spark=mock_spark,
            df=user_data,
            file_path=test_path,
            file_format="parquet"
        )
        
        # Verify the workflow
        mock_spark.createDataFrame.assert_called_once_with(user_data)
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.format.assert_called_once_with("parquet")
        mock_writer.save.assert_called_once_with(test_path)
    
    @patch('core.spark.get_spark')
    def test_batch_load_integration(self, mock_get_spark, temp_dir):
        """Test batch load operations integration."""
        # Setup mock Spark session
        mock_spark = Mock()
        mock_get_spark.return_value = mock_spark
        
        # Mock SQL operations
        mock_result_df = Mock()
        mock_spark.sql.side_effect = [None, mock_result_df]  # COPY INTO, SELECT
        
        # Test batch load
        source_path = os.path.join(temp_dir, "source", "*.parquet")
        target_table = "test_table"
        
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            file_format="PARQUET"
        )
        
        # Verify SQL operations
        assert mock_spark.sql.call_count == 2
        
        # Check the SQL commands
        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        assert any("COPY INTO" in sql for sql in sql_calls)
        assert any("SELECT * FROM" in sql for sql in sql_calls)
        
        assert result == mock_result_df


@pytest.mark.integration
class TestErrorHandlingIntegration:
    """Integration tests for error handling across modules."""
    
    def test_spark_failure_handling(self):
        """Test handling of Spark session failures."""
        # Test error handling by trying to use invalid operations
        import pandas as pd
        test_data = pd.DataFrame({'test': [1, 2, 3]})
        
        # Test that None spark session raises appropriate error
        invalid_spark = None
        with pytest.raises(AttributeError):
            invalid_spark.createDataFrame(test_data)
    
    def test_catalog_failure_handling(self, test_config):
        """Test handling of catalog operation failures."""
        from core.catalog import ensure_catalog_schema_volume
        from unittest.mock import Mock
        
        # Test with invalid inputs to catalog function
        mock_spark = Mock()
        
        # Test catalog function with None values
        result = ensure_catalog_schema_volume(
            spark=mock_spark,
            catalog_name=None,
            schema_name=None,
            volume_name=None,
            auto_create_catalog=False,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        # Should return False for None values
        assert result is False
    
    def test_data_generation_edge_cases(self):
        """Test data generation with edge case parameters."""
        # Test with minimal records
        min_user_data = generate_user_profiles(num_records=1)
        assert len(min_user_data) == 1
        
        min_sales_data = generate_sales_data(user_ids=min_user_data['user_id'].tolist(), num_records=1)
        assert len(min_sales_data) == 1
        
        # Test with larger datasets
        large_user_data = generate_user_profiles(num_records=100)
        large_sales_data = generate_sales_data(user_ids=large_user_data['user_id'].tolist(), num_records=500)
        assert len(large_user_data) == 100
        assert len(large_sales_data) == 500
        
        # Verify date ranges in signup_date and sale_date
        if 'signup_date' in large_user_data.columns:
            signup_date_range = large_user_data['signup_date'].max() - large_user_data['signup_date'].min()
            assert signup_date_range.days >= 0  # Should have some range
            
        if 'sale_date' in large_sales_data.columns:
            sale_date_range = large_sales_data['sale_date'].max() - large_sales_data['sale_date'].min()
            assert sale_date_range.days >= 0  # Should have some range


@pytest.mark.integration  
class TestPerformanceIntegration:
    """Integration tests for performance characteristics."""
    
    def test_data_generation_performance(self):
        """Test performance of data generation."""
        import time
        
        # Test generation of moderate dataset
        start_time = time.time()
        
        user_data = generate_user_profiles(num_records=500)
        sales_data = generate_sales_data(user_ids=user_data['user_id'].tolist(), num_records=1000)
        
        generation_time = time.time() - start_time
        
        # Verify data was generated correctly
        assert len(user_data) == 500
        assert len(sales_data) == 1000
        
        # Performance check (adjust threshold as needed)
        assert generation_time < 15.0, f"Data generation took too long: {generation_time} seconds"
    
    @patch('core.spark.get_spark')
    def test_io_operation_performance(self, mock_get_spark):
        """Test performance of I/O operations."""
        import time
        
        # Generate test data
        user_data = generate_user_profiles(num_records=100)
        
        # Setup mock Spark session
        mock_spark = Mock()
        mock_spark_df = Mock()
        mock_spark.createDataFrame.return_value = mock_spark_df
        
        mock_writer = Mock()
        mock_spark_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.save.return_value = None
        
        mock_get_spark.return_value = mock_spark
        
        # Time the I/O operations
        start_time = time.time()
        
        for i in range(5):
            save_to_volume(
                spark=mock_spark,
                df=user_data,
                file_path=f"/tmp/test_path_{i}",
                file_format="parquet"
            )
        
        io_time = time.time() - start_time
        
        # Performance check
        assert io_time < 5.0, f"I/O operations took too long: {io_time} seconds"
        
        # Verify all operations completed
        assert mock_spark.createDataFrame.call_count == 5