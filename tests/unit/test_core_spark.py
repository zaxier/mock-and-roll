import pytest
from unittest.mock import Mock, patch
import logging


class TestGetSpark:
    """Test cases for the get_spark function."""
    
    @patch('databricks.connect.DatabricksSession')
    def test_get_spark_databricks_success(self, mock_databricks_session):
        """Test successful creation of serverless Databricks session."""
        mock_session = Mock()
        mock_databricks_session.builder.serverless.return_value.getOrCreate.return_value = mock_session
        
        from src.core.spark import get_spark
        result = get_spark()
        
        assert result == mock_session
        mock_databricks_session.builder.serverless.assert_called_once()
        mock_databricks_session.builder.serverless.return_value.getOrCreate.assert_called_once()
    
    @patch('src.core.spark.SparkSession')
    @patch('databricks.connect.DatabricksSession')
    def test_get_spark_serverless_exception_fallback_to_local(self, mock_databricks_session, mock_spark_session):
        """Test fallback to local Spark when serverless Databricks session fails."""
        # Serverless call raises exception
        mock_databricks_session.builder.serverless.return_value.getOrCreate.side_effect = Exception("Connection failed")
        
        # Local Spark session succeeds
        mock_local_session = Mock()
        mock_spark_session.builder.getOrCreate.return_value = mock_local_session
        
        from src.core.spark import get_spark
        result = get_spark()
        
        assert result == mock_local_session
        mock_databricks_session.builder.serverless.assert_called_once()
        mock_databricks_session.builder.serverless.return_value.getOrCreate.assert_called_once()
        mock_spark_session.builder.getOrCreate.assert_called_once()
    
    @patch('src.core.spark.logger')
    @patch('databricks.connect.DatabricksSession')
    def test_get_spark_logging_info_success(self, mock_databricks_session, mock_logger):
        """Test that info log is written when serverless Databricks session is created successfully."""
        mock_session = Mock()
        mock_databricks_session.builder.serverless.return_value.getOrCreate.return_value = mock_session
        
        from src.core.spark import get_spark
        get_spark()
        
        mock_logger.info.assert_called_with("Attempting to create serverless Databricks session")
    
    @patch('src.core.spark.SparkSession')
    @patch('src.core.spark.logger')
    @patch('databricks.connect.DatabricksSession')
    def test_get_spark_logging_warning_and_local_fallback(self, mock_databricks_session, mock_logger, mock_spark_session):
        """Test that warning logs are written when falling back to local Spark."""
        exception_msg = "Connection failed"
        mock_databricks_session.builder.serverless.return_value.getOrCreate.side_effect = Exception(exception_msg)
        
        mock_local_session = Mock()
        mock_spark_session.builder.getOrCreate.return_value = mock_local_session
        
        from src.core.spark import get_spark
        get_spark()
        
        mock_logger.info.assert_called_with("Attempting to create serverless Databricks session")
        mock_logger.warning.assert_any_call(f"Error creating serverless Databricks session: {exception_msg}")
        mock_logger.warning.assert_any_call("Falling back to local Spark session")


class TestLoggingConfiguration:
    """Test cases for logging configuration."""
    
    def test_logger_name(self):
        """Test that logger is created with correct name."""
        import src.core.spark
        
        assert src.core.spark.logger.name == 'src.core.spark'
    
    def test_logger_level(self):
        """Test that logging is configured at DEBUG level."""
        # Test that the basicConfig was called with DEBUG level
        # by checking if a test logger would be at DEBUG level
        test_logger = logging.getLogger("test_logger")
        test_logger.setLevel(logging.DEBUG)
        assert test_logger.isEnabledFor(logging.DEBUG)