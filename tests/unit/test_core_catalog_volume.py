"""
Tests for volume auto-creation functionality in core.catalog module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession

from src.core.catalog import (
    volume_exists,
    create_volume,
    ensure_catalog_schema_volume
)


@pytest.mark.unit
class TestVolumeExists:
    """Test volume_exists function."""
    
    def test_volume_exists_true(self):
        """Test when volume exists."""
        mock_spark = Mock(spec=SparkSession)
        mock_row = Mock()
        mock_row.volume_name = "test_volume"
        mock_spark.sql.return_value.collect.return_value = [mock_row]
        
        result = volume_exists(mock_spark, "test_catalog", "test_schema", "test_volume")
        
        assert result == True
        mock_spark.sql.assert_called_once_with("SHOW VOLUMES IN `test_catalog`.`test_schema`")
    
    def test_volume_exists_false(self):
        """Test when volume does not exist."""
        mock_spark = Mock(spec=SparkSession)
        mock_row = Mock()
        mock_row.volume_name = "other_volume"
        mock_spark.sql.return_value.collect.return_value = [mock_row]
        
        result = volume_exists(mock_spark, "test_catalog", "test_schema", "test_volume")
        
        assert result == False
        mock_spark.sql.assert_called_once_with("SHOW VOLUMES IN `test_catalog`.`test_schema`")
    
    def test_volume_exists_exception(self):
        """Test when SQL query raises exception."""
        mock_spark = Mock(spec=SparkSession)
        mock_spark.sql.side_effect = Exception("SQL Error")
        
        result = volume_exists(mock_spark, "test_catalog", "test_schema", "test_volume")
        
        assert result == False


@pytest.mark.unit
class TestCreateVolume:
    """Test create_volume function."""
    
    @patch('src.core.catalog.volume_exists')
    def test_create_volume_already_exists(self, mock_volume_exists):
        """Test when volume already exists."""
        mock_spark = Mock(spec=SparkSession)
        mock_volume_exists.return_value = True
        
        result = create_volume(mock_spark, "test_catalog", "test_schema", "test_volume")
        
        assert result == True
        mock_spark.sql.assert_not_called()
    
    @patch('src.core.catalog.volume_exists')
    def test_create_volume_success(self, mock_volume_exists):
        """Test successful volume creation."""
        mock_spark = Mock(spec=SparkSession)
        mock_volume_exists.return_value = False
        
        result = create_volume(mock_spark, "test_catalog", "test_schema", "test_volume")
        
        assert result == True
        expected_sql = "CREATE VOLUME IF NOT EXISTS `test_catalog`.`test_schema`.`test_volume`"
        mock_spark.sql.assert_called_once_with(expected_sql)
    
    @patch('src.core.catalog.volume_exists')
    def test_create_volume_with_comment(self, mock_volume_exists):
        """Test volume creation with comment."""
        mock_spark = Mock(spec=SparkSession)
        mock_volume_exists.return_value = False
        
        result = create_volume(mock_spark, "test_catalog", "test_schema", "test_volume", "Test comment")
        
        assert result == True
        expected_sql = "CREATE VOLUME IF NOT EXISTS `test_catalog`.`test_schema`.`test_volume` COMMENT 'Test comment'"
        mock_spark.sql.assert_called_once_with(expected_sql)
    
    @patch('src.core.catalog.volume_exists')
    def test_create_volume_exception(self, mock_volume_exists):
        """Test volume creation failure."""
        mock_spark = Mock(spec=SparkSession)
        mock_volume_exists.return_value = False
        mock_spark.sql.side_effect = Exception("Creation failed")
        
        result = create_volume(mock_spark, "test_catalog", "test_schema", "test_volume")
        
        assert result == False


@pytest.mark.unit
class TestEnsureCatalogSchemaVolume:
    """Test ensure_catalog_schema_volume function with volume auto-creation."""
    
    @patch('src.core.catalog.catalog_exists')
    @patch('src.core.catalog.schema_exists')
    @patch('src.core.catalog.volume_exists')
    def test_all_resources_exist(self, mock_volume_exists, mock_schema_exists, mock_catalog_exists):
        """Test when all resources already exist."""
        mock_spark = Mock(spec=SparkSession)
        mock_catalog_exists.return_value = True
        mock_schema_exists.return_value = True
        mock_volume_exists.return_value = True
        
        result = ensure_catalog_schema_volume(
            mock_spark, "test_catalog", "test_schema", "test_volume"
        )
        
        assert result == True
    
    @patch('src.core.catalog.catalog_exists')
    @patch('src.core.catalog.schema_exists')
    @patch('src.core.catalog.volume_exists')
    @patch('src.core.catalog.create_volume')
    def test_auto_create_volume_true(self, mock_create_volume, mock_volume_exists, 
                                    mock_schema_exists, mock_catalog_exists):
        """Test volume auto-creation when enabled."""
        mock_spark = Mock(spec=SparkSession)
        mock_catalog_exists.return_value = True
        mock_schema_exists.return_value = True
        mock_volume_exists.return_value = False
        mock_create_volume.return_value = True
        
        result = ensure_catalog_schema_volume(
            mock_spark, "test_catalog", "test_schema", "test_volume",
            auto_create_volume=True
        )
        
        assert result == True
        mock_create_volume.assert_called_once_with(
            mock_spark, "test_catalog", "test_schema", "test_volume",
            "Auto-created volume for demo data"
        )
    
    @patch('src.core.catalog.catalog_exists')
    @patch('src.core.catalog.schema_exists')
    @patch('src.core.catalog.volume_exists')
    @patch('src.core.catalog.create_volume')
    def test_auto_create_volume_false(self, mock_create_volume, mock_volume_exists,
                                     mock_schema_exists, mock_catalog_exists):
        """Test volume auto-creation when disabled."""
        mock_spark = Mock(spec=SparkSession)
        mock_catalog_exists.return_value = True
        mock_schema_exists.return_value = True
        mock_volume_exists.return_value = False
        
        result = ensure_catalog_schema_volume(
            mock_spark, "test_catalog", "test_schema", "test_volume",
            auto_create_volume=False
        )
        
        assert result == False
        mock_create_volume.assert_not_called()
    
    @patch('src.core.catalog.catalog_exists')
    @patch('src.core.catalog.schema_exists')
    @patch('src.core.catalog.volume_exists')
    @patch('src.core.catalog.create_volume')
    def test_volume_creation_fails(self, mock_create_volume, mock_volume_exists,
                                  mock_schema_exists, mock_catalog_exists):
        """Test when volume creation fails."""
        mock_spark = Mock(spec=SparkSession)
        mock_catalog_exists.return_value = True
        mock_schema_exists.return_value = True
        mock_volume_exists.return_value = False
        mock_create_volume.return_value = False
        
        result = ensure_catalog_schema_volume(
            mock_spark, "test_catalog", "test_schema", "test_volume",
            auto_create_volume=True
        )
        
        assert result == False
        mock_create_volume.assert_called_once()
    
    @patch('src.core.catalog.catalog_exists')
    @patch('src.core.catalog.create_catalog')
    @patch('src.core.catalog.schema_exists')
    @patch('src.core.catalog.create_schema')
    @patch('src.core.catalog.volume_exists')
    @patch('src.core.catalog.create_volume')
    def test_full_auto_creation_chain(self, mock_create_volume, mock_volume_exists,
                                     mock_create_schema, mock_schema_exists,
                                     mock_create_catalog, mock_catalog_exists):
        """Test full auto-creation chain for all resources."""
        mock_spark = Mock(spec=SparkSession)
        
        # All resources don't exist initially
        mock_catalog_exists.return_value = False
        mock_schema_exists.return_value = False
        mock_volume_exists.return_value = False
        
        # All creation operations succeed
        mock_create_catalog.return_value = True
        mock_create_schema.return_value = True
        mock_create_volume.return_value = True
        
        result = ensure_catalog_schema_volume(
            mock_spark, "test_catalog", "test_schema", "test_volume",
            auto_create_catalog=True,
            auto_create_schema=True,
            auto_create_volume=True
        )
        
        assert result == True
        mock_create_catalog.assert_called_once()
        mock_create_schema.assert_called_once()
        mock_create_volume.assert_called_once()