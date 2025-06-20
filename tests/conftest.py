"""
Shared test fixtures for both unit and integration tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import pandas as pd
from datetime import datetime, timedelta

from config import get_config, Config
from core.spark import get_spark


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for integration testing.
    Attempts to use local mode, falls back to mocking if local mode is not available.
    """
    try:
        # Try to create local Spark session
        import os
        # Temporarily disable remote-only mode for testing
        old_env = os.environ.get("SPARK_CONNECT_MODE_ENABLED")
        if old_env:
            del os.environ["SPARK_CONNECT_MODE_ENABLED"]
        
        spark = (SparkSession.builder
                 .appName("IntegrationTests")
                 .master("local[1]")
                 .config("spark.ui.enabled", "false")
                 .config("spark.driver.host", "localhost")
                 .getOrCreate())
        
        # Set log level to reduce noise during testing
        spark.sparkContext.setLogLevel("WARN")
        
        yield spark
        
        # Cleanup
        spark.stop()
        
        # Restore environment
        if old_env:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = old_env
    
    except Exception as e:
        # If local Spark fails, use the existing get_spark from our core module
        from core.spark import get_spark
        spark = get_spark()
        yield spark
        # Note: Don't stop this one as it may be a shared session


@pytest.fixture(scope="function")
def temp_dir():
    """
    Create a temporary directory for test files.
    Automatically cleaned up after each test.
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="function")
def test_config():
    """
    Create a test configuration for integration tests.
    Uses temporary directories and test-safe settings.
    """
    config = get_config()
    
    # Create a proper mock with nested attributes
    test_config = Mock()
    
    # Mock databricks section
    test_config.databricks = Mock()
    test_config.databricks.catalog = "test_catalog"
    test_config.databricks.schema = "test_schema"
    test_config.databricks.volume = "test_volume"
    test_config.databricks.auto_create_catalog = True
    test_config.databricks.auto_create_schema = True
    test_config.databricks.auto_create_volume = True
    
    # Mock storage section
    test_config.storage = Mock()
    test_config.storage.default_format = "parquet"
    
    # Mock data_generation section
    test_config.data_generation = Mock()
    test_config.data_generation.default_records = 100
    test_config.data_generation.date_range_days = 30
    
    # Mock logging section
    test_config.logging = Mock()
    test_config.logging.level = "INFO"
    test_config.logging.format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Mock get_volume_path method
    def mock_get_volume_path(subpath=""):
        base_path = f"/tmp/test_volumes/{test_config.databricks.catalog}/{test_config.databricks.schema}/{test_config.databricks.volume}"
        return f"{base_path}/{subpath}" if subpath else base_path
    
    test_config.databricks.get_volume_path = mock_get_volume_path
    
    return test_config


@pytest.fixture(scope="function")
def sample_financial_data():
    """
    Generate sample financial transaction data for testing.
    """
    data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(50):
        data.append({
            'transaction_id': f"TXN-{i+1:06d}",
            'customer_name': f"Customer {i+1}",
            'customer_email': f"customer{i+1}@example.com",
            'amount': round(100.0 + (i * 10.5), 2),
            'currency': 'USD' if i % 2 == 0 else 'EUR',
            'transaction_date': base_date + timedelta(days=i % 30),
            'merchant_name': f"Merchant {(i % 5) + 1}",
            'category': ['Retail', 'Food', 'Gas', 'Entertainment', 'Online'][i % 5],
            'payment_method': ['VISA', 'MASTERCARD', 'AMEX'][i % 3],
            'city': ['New York', 'Los Angeles', 'Chicago', 'Houston'][i % 4],
            'state': ['NY', 'CA', 'IL', 'TX'][i % 4],
            'country': 'USA'
        })
    
    return pd.DataFrame(data)


@pytest.fixture(scope="function")
def financial_transactions_schema():
    """
    Define the schema for financial transactions data.
    """
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True)
    ])


@pytest.fixture(scope="function")
def mock_spark_session():
    """
    Create a mock Spark session for unit tests.
    """
    mock_spark = Mock(spec=SparkSession)
    mock_spark.sql.return_value = Mock()
    mock_spark.createDataFrame.return_value = Mock()
    mock_spark.readStream = Mock()
    mock_spark.stop.return_value = None
    return mock_spark


# Markers for different test types
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "spark: mark test as requiring Spark")
    config.addinivalue_line("markers", "databricks: mark test as requiring Databricks")


# Conditional fixtures based on environment
@pytest.fixture(scope="session")
def databricks_available():
    """
    Check if Databricks environment is available for testing.
    """
    try:
        import databricks
        # Add additional checks for Databricks connectivity if needed
        return True
    except ImportError:
        return False


@pytest.fixture(scope="function")
def clean_spark_tables(spark_session):
    """
    Fixture to clean up any test tables created during tests.
    """
    created_tables = []
    
    def register_table(table_name):
        created_tables.append(table_name)
    
    yield register_table
    
    # Cleanup
    for table_name in created_tables:
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Ignore cleanup errors