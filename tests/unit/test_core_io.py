import pytest
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.core.io import save_to_volume, read_stream_with_autoloader, batch_load_with_copy_into
# from core.io import save_to_volume, read_stream_with_autoloader


class TestSaveToVolume:
    """Test cases for the save_to_volume function."""
    
    def test_save_pandas_dataframe_to_volume(self):
        """Test saving a pandas DataFrame to volume."""
        # Setup
        mock_spark = Mock()
        mock_spark_df = Mock()
        mock_spark.createDataFrame.return_value = mock_spark_df
        
        mock_writer = Mock()
        mock_spark_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        
        pandas_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        file_path = "/volumes/catalog/schema/volume/test.parquet"
        
        # Execute
        save_to_volume(mock_spark, pandas_df, file_path)
        
        # Verify
        mock_spark.createDataFrame.assert_called_once_with(pandas_df)
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.format.assert_called_once_with("parquet")
        mock_writer.save.assert_called_once_with(file_path)
    
    def test_save_spark_dataframe_to_volume(self):
        """Test saving a Spark DataFrame to volume."""
        # Setup
        mock_spark = Mock()
        mock_spark_df = Mock(spec=DataFrame)
        mock_writer = Mock()
        mock_spark_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        
        file_path = "/volumes/catalog/schema/volume/test.csv"
        file_format = "csv"
        
        # Execute
        save_to_volume(mock_spark, mock_spark_df, file_path, file_format)
        
        # Verify
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.format.assert_called_once_with("csv")
        mock_writer.save.assert_called_once_with(file_path)
    
    def test_save_to_volume_custom_format(self):
        """Test saving with custom file format."""
        # Setup
        mock_spark = Mock()
        mock_spark_df = Mock()
        mock_spark.createDataFrame.return_value = mock_spark_df
        
        mock_writer = Mock()
        mock_spark_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        
        pandas_df = pd.DataFrame({'col1': [1, 2, 3]})
        file_path = "/volumes/catalog/schema/volume/test.json"
        file_format = "json"
        
        # Execute
        save_to_volume(mock_spark, pandas_df, file_path, file_format)
        
        # Verify
        mock_writer.format.assert_called_once_with("json")
    
    def test_save_to_volume_pandas_conversion_error(self):
        """Test error handling when pandas to Spark conversion fails."""
        # Setup
        mock_spark = Mock()
        mock_spark.createDataFrame.side_effect = Exception("Conversion failed")
        
        pandas_df = pd.DataFrame({'col1': [1, 2, 3]})
        file_path = "/volumes/catalog/schema/volume/test.parquet"
        
        # Execute & Verify
        with pytest.raises(Exception, match="Conversion failed"):
            save_to_volume(mock_spark, pandas_df, file_path)


class TestReadStreamWithAutoloader:
    """Test cases for the read_stream_with_autoloader function."""
    
    def test_read_stream_with_autoloader_success(self):
        """Test successful reading with autoloader."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_read_stream = Mock()
        mock_spark.readStream = mock_read_stream
        
        mock_df = Mock()
        mock_df_with_timestamp = Mock()
        mock_df_with_file = Mock()
        
        # Chain the mock calls
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.schema.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_df
        
        mock_df.withColumn.return_value = mock_df_with_timestamp
        mock_df_with_timestamp.withColumn.return_value = mock_df_with_file
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        source_path = "/volumes/catalog/schema/volume/source/*"
        file_format = "csv"
        
        # Execute
        result = read_stream_with_autoloader(mock_spark, source_path, schema, file_format)
        
        # Verify
        mock_read_stream.format.assert_called_once_with("cloudFiles")
        
        # Check all option calls
        expected_option_calls = [
            (("cloudFiles.format", "csv"),),
            (("cloudFiles.inferColumnTypes", "true"),),
            (("cloudFiles.schemaLocation", "dbfs:/volumes/catalog/schema/volume/source/*/_schema"),),
            (("cloudFiles.partitionColumns", ""),)
        ]
        assert mock_read_stream.option.call_count == 4
        for expected_call in expected_option_calls:
            mock_read_stream.option.assert_any_call(*expected_call[0])
        
        mock_read_stream.schema.assert_called_once_with(schema)
        mock_read_stream.load.assert_called_once_with("dbfs:/volumes/catalog/schema/volume/source/*")
        
        # Check that metadata columns are added
        assert mock_df.withColumn.call_count == 1
        assert mock_df_with_timestamp.withColumn.call_count == 1
        
        # Check the column names
        calls = mock_df.withColumn.call_args_list + mock_df_with_timestamp.withColumn.call_args_list
        column_names = [call[0][0] for call in calls]
        assert "_ingestion_timestamp" in column_names
        assert "_source_file" in column_names
        
        assert result == mock_df_with_file
    
    def test_read_stream_with_autoloader_default_format(self):
        """Test reading with default parquet format."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_read_stream = Mock()
        mock_spark.readStream = mock_read_stream
        
        mock_df = Mock()
        mock_df_with_timestamp = Mock()
        mock_df_with_file = Mock()
        
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.schema.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_df
        
        mock_df.withColumn.return_value = mock_df_with_timestamp
        mock_df_with_timestamp.withColumn.return_value = mock_df_with_file
        
        schema = StructType([StructField("id", IntegerType(), True)])
        source_path = "/volumes/catalog/schema/volume/source/*"
        
        # Execute (without specifying format)
        result = read_stream_with_autoloader(mock_spark, source_path, schema)
        
        # Verify default format is used
        expected_option_calls = [
            (("cloudFiles.format", "parquet"),),
            (("cloudFiles.inferColumnTypes", "true"),),
            (("cloudFiles.schemaLocation", "dbfs:/volumes/catalog/schema/volume/source/*/_schema"),),
            (("cloudFiles.partitionColumns", ""),)
        ]
        assert mock_read_stream.option.call_count == 4
        for expected_call in expected_option_calls:
            mock_read_stream.option.assert_any_call(*expected_call[0])
    
    def test_read_stream_with_autoloader_json_format(self):
        """Test reading with JSON format."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_read_stream = Mock()
        mock_spark.readStream = mock_read_stream
        
        mock_df = Mock()
        mock_df_with_timestamp = Mock()
        mock_df_with_file = Mock()
        
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.schema.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_df
        
        mock_df.withColumn.return_value = mock_df_with_timestamp
        mock_df_with_timestamp.withColumn.return_value = mock_df_with_file
        
        schema = StructType([StructField("data", StringType(), True)])
        source_path = "/volumes/catalog/schema/volume/json_source/*"
        file_format = "json"
        
        # Execute
        result = read_stream_with_autoloader(mock_spark, source_path, schema, file_format)
        
        # Verify JSON format is used
        expected_option_calls = [
            (("cloudFiles.format", "json"),),
            (("cloudFiles.inferColumnTypes", "true"),),
            (("cloudFiles.schemaLocation", "dbfs:/volumes/catalog/schema/volume/json_source/*/_schema"),),
            (("cloudFiles.partitionColumns", ""),)
        ]
        assert mock_read_stream.option.call_count == 4
        for expected_call in expected_option_calls:
            mock_read_stream.option.assert_any_call(*expected_call[0])
    
    @patch('src.core.io.current_timestamp')
    @patch('src.core.io.col')
    def test_read_stream_metadata_columns(self, mock_col, mock_current_timestamp):
        """Test that metadata columns are added correctly."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_read_stream = Mock()
        mock_spark.readStream = mock_read_stream
        
        mock_df = Mock()
        mock_df_with_timestamp = Mock()
        mock_df_with_file = Mock()
        
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.schema.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_df
        
        mock_df.withColumn.return_value = mock_df_with_timestamp
        mock_df_with_timestamp.withColumn.return_value = mock_df_with_file
        
        mock_timestamp_col = Mock()
        mock_file_col = Mock()
        mock_current_timestamp.return_value = mock_timestamp_col
        mock_col.return_value = mock_file_col
        
        schema = StructType([])
        source_path = "/test/path"
        
        # Execute
        result = read_stream_with_autoloader(mock_spark, source_path, schema)
        
        # Verify metadata columns
        mock_current_timestamp.assert_called_once()
        mock_col.assert_called_once_with("_metadata.file_path")
        
        # Check withColumn calls
        mock_df.withColumn.assert_called_once_with("_ingestion_timestamp", mock_timestamp_col)
        mock_df_with_timestamp.withColumn.assert_called_once_with("_source_file", mock_file_col)
        
        assert result == mock_df_with_file


class TestIntegrationScenarios:
    """Integration test scenarios for the IO module."""
    
    def test_end_to_end_pandas_to_volume_scenario(self):
        """Test complete scenario of saving pandas DataFrame to volume."""
        # Setup
        mock_spark = Mock()
        mock_spark_df = Mock()
        mock_spark.createDataFrame.return_value = mock_spark_df
        
        # Mock the write chain
        mock_writer = Mock()
        mock_spark_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        
        # Test data
        test_data = pd.DataFrame({
            'transaction_id': [1, 2, 3],
            'amount': [100.0, 250.0, 75.0],
            'currency': ['USD', 'EUR', 'GBP']
        })
        
        volume_path = "/volumes/dev/financial/transactions/raw_transactions.parquet"
        
        # Execute
        save_to_volume(mock_spark, test_data, volume_path, "parquet")
        
        # Verify complete chain
        mock_spark.createDataFrame.assert_called_once_with(test_data)
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.format.assert_called_once_with("parquet")
        mock_writer.save.assert_called_once_with(volume_path)
    
    def test_autoloader_with_complex_schema(self):
        """Test autoloader with complex schema structure."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_read_stream = Mock()
        mock_spark.readStream = mock_read_stream
        
        mock_df = Mock()
        mock_df_with_timestamp = Mock()
        mock_df_with_file = Mock()
        
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.schema.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_df
        
        mock_df.withColumn.return_value = mock_df_with_timestamp
        mock_df_with_timestamp.withColumn.return_value = mock_df_with_file
        
        # Complex schema
        complex_schema = StructType([
            StructField("transaction_id", IntegerType(), False),
            StructField("customer_name", StringType(), True),
            StructField("amount", StringType(), True),  # String to handle decimals
            StructField("transaction_date", StringType(), True),
            StructField("category", StringType(), True)
        ])
        
        source_path = "/volumes/dev/financial/transactions/bronze/*"
        
        # Execute
        result = read_stream_with_autoloader(mock_spark, source_path, complex_schema, "csv")
        
        # Verify
        mock_read_stream.schema.assert_called_once_with(complex_schema)
        
        # Check all option calls
        expected_option_calls = [
            (("cloudFiles.format", "csv"),),
            (("cloudFiles.inferColumnTypes", "true"),),
            (("cloudFiles.schemaLocation", "dbfs:/volumes/dev/financial/transactions/bronze/*/_schema"),),
            (("cloudFiles.partitionColumns", ""),)
        ]
        assert mock_read_stream.option.call_count == 4
        for expected_call in expected_option_calls:
            mock_read_stream.option.assert_any_call(*expected_call[0])
        
        assert result == mock_df_with_file


class TestBatchLoadWithCopyInto:
    """Test cases for the batch_load_with_copy_into function."""
    
    def test_batch_load_basic_success(self):
        """Test basic successful batch load with COPY INTO."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        
        # Mock SQL executions - without schema, only drop, copy, and select
        mock_spark.sql.side_effect = [
            None,  # DROP TABLE IF EXISTS (no return)
            None,  # COPY INTO (no return)
            mock_result_df  # SELECT * FROM table
        ]
        
        source_path = "/volumes/catalog/schema/volume/data/*.parquet"
        target_table = "catalog.schema.test_table"
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            drop_table_if_exists=True
        )
        
        # Verify
        expected_calls = [
            call(f"DROP TABLE IF EXISTS {target_table}"),
            call(f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = PARQUET"),
            call(f"SELECT * FROM {target_table}")
        ]
        
        # Check that correct SQL commands were called
        assert mock_spark.sql.call_count == 3
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df
    
    def test_batch_load_with_schema_creation(self):
        """Test batch load with automatic table creation using provided schema."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        
        # Mock that table doesn't exist (SHOW TABLES returns empty)
        mock_show_result = Mock()
        mock_show_result.count.return_value = 0  # Table doesn't exist
        
        mock_spark.sql.side_effect = [
            mock_show_result,  # SHOW TABLES returns empty result
            None,  # CREATE TABLE succeeds
            None,  # COPY INTO succeeds
            mock_result_df  # SELECT * succeeds
        ]
        
        # Define test schema
        test_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("amount", StringType(), True)
        ])
        
        source_path = "/volumes/test/data/*.json"
        target_table = "test_catalog.test_schema.test_table"
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            file_format="JSON",
            table_schema=test_schema
        )
        
        # Verify
        expected_calls = [
            call("SHOW TABLES IN test_catalog.test_schema LIKE 'test_table'"),  # Check if table exists
            call("CREATE TABLE test_catalog.test_schema.test_table (id INT NOT NULL, name STRING, amount STRING)"),
            call(f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = JSON"),
            call(f"SELECT * FROM {target_table}")
        ]
        
        assert mock_spark.sql.call_count == 4
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df
    
    def test_batch_load_table_already_exists_with_schema(self):
        """Test batch load when table already exists and schema is provided."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        mock_show_result = Mock()
        mock_show_result.count.return_value = 1  # Table exists
        
        # Mock that table exists (SHOW TABLES returns result)
        mock_spark.sql.side_effect = [
            mock_show_result,  # SHOW TABLES returns 1 row (table exists)
            None,  # COPY INTO succeeds
            mock_result_df  # SELECT * succeeds
        ]
        
        test_schema = StructType([
            StructField("col1", StringType(), True)
        ])
        
        source_path = "/test/path/*.csv"
        target_table = "existing.table"
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            file_format="CSV",
            table_schema=test_schema
        )
        
        # Verify - should not create table since it exists
        expected_calls = [
            call("SHOW TABLES IN existing LIKE 'table'"),  # Check if table exists
            call(f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = CSV"),
            call(f"SELECT * FROM {target_table}")
        ]
        
        assert mock_spark.sql.call_count == 3
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df
    
    def test_batch_load_with_copy_options(self):
        """Test batch load with additional COPY INTO options."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        
        mock_spark.sql.side_effect = [
            None,  # COPY INTO
            mock_result_df  # SELECT *
        ]
        
        source_path = "/volumes/test/data/*.csv"
        target_table = "test.table"
        copy_options = {
            "delimiter": ",",
            "header": "true",
            "quote": '"'
        }
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            file_format="CSV",
            copy_options=copy_options
        )
        
        # Verify
        expected_copy_sql = (
            f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = CSV "
            f"OPTIONS (delimiter = ',', header = 'true', quote = '\"')"
        )
        
        expected_calls = [
            call(expected_copy_sql),
            call(f"SELECT * FROM {target_table}")
        ]
        
        assert mock_spark.sql.call_count == 2
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df
    
    def test_batch_load_copy_into_failure(self):
        """Test error handling when COPY INTO command fails."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        
        copy_error = Exception("COPY INTO failed: invalid file format")
        mock_spark.sql.side_effect = copy_error
        
        source_path = "/invalid/path/*.txt"
        target_table = "test.table"
        
        # Execute & Verify
        with pytest.raises(Exception, match="COPY INTO failed: invalid file format"):
            batch_load_with_copy_into(
                spark=mock_spark,
                source_path=source_path,
                target_table=target_table
            )
        
        # Should have attempted the COPY INTO command
        expected_call = call(f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = PARQUET")
        mock_spark.sql.assert_called_once_with(expected_call.args[0])
    
    def test_batch_load_drop_table_only(self):
        """Test batch load with drop table but no schema creation."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        
        mock_spark.sql.side_effect = [
            None,  # DROP TABLE IF EXISTS
            None,  # COPY INTO
            mock_result_df  # SELECT *
        ]
        
        source_path = "/test/data/*.parquet"
        target_table = "test.drop_table"
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            drop_table_if_exists=True
        )
        
        # Verify
        expected_calls = [
            call(f"DROP TABLE IF EXISTS {target_table}"),
            call(f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = PARQUET"),
            call(f"SELECT * FROM {target_table}")
        ]
        
        assert mock_spark.sql.call_count == 3
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df
    
    @patch('src.core.io.logger')
    def test_batch_load_logging(self, mock_logger):
        """Test that appropriate logging occurs during batch load."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        
        mock_spark.sql.side_effect = [
            None,  # DROP TABLE
            None,  # COPY INTO
            mock_result_df  # SELECT *
        ]
        
        source_path = "/test/path/*.json"
        target_table = "test.logging_table"
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            file_format="JSON",
            drop_table_if_exists=True
        )
        
        # Verify logging calls - using debug level for drop table and copy into, info for success
        mock_logger.debug.assert_any_call(f"Dropping table if exists: {target_table}")
        mock_logger.debug.assert_any_call(f"Executing COPY INTO: COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = JSON")
        mock_logger.info.assert_any_call(f"Successfully loaded data into {target_table}")
        
        assert result == mock_result_df
    
    def test_batch_load_default_parameters(self):
        """Test batch load with all default parameters."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        
        mock_spark.sql.side_effect = [
            None,  # COPY INTO
            mock_result_df  # SELECT *
        ]
        
        source_path = "/default/path/*.parquet"
        target_table = "default.table"
        
        # Execute - using all defaults
        result = batch_load_with_copy_into(mock_spark, source_path, target_table)
        
        # Verify defaults: PARQUET format, no table drop, no schema creation, no options
        expected_calls = [
            call(f"COPY INTO {target_table} FROM '{source_path}' FILEFORMAT = PARQUET"),
            call(f"SELECT * FROM {target_table}")
        ]
        
        assert mock_spark.sql.call_count == 2
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df


class TestBatchLoadIntegrationScenarios:
    """Integration test scenarios for batch_load_with_copy_into function."""
    
    def test_complete_table_recreation_workflow(self):
        """Test complete workflow: drop table, create with schema, load data."""
        # Setup
        mock_spark = Mock(spec=SparkSession)
        mock_result_df = Mock(spec=DataFrame)
        mock_show_result = Mock()
        mock_show_result.count.return_value = 0  # Table doesn't exist after drop
        
        # Simulate table doesn't exist after drop
        mock_spark.sql.side_effect = [
            None,  # DROP TABLE IF EXISTS
            mock_show_result,  # SHOW TABLES (table doesn't exist)
            None,  # CREATE TABLE
            None,  # COPY INTO
            mock_result_df  # SELECT *
        ]
        
        # Complex financial transactions schema
        financial_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("transaction_date", StringType(), True)
        ])
        
        source_path = "/volumes/dev/financial/bronze/*.json"
        target_table = "dev.financial.transactions_copy"
        copy_options = {"multiline": "true", "timestampFormat": "yyyy-MM-dd HH:mm:ss"}
        
        # Execute
        result = batch_load_with_copy_into(
            spark=mock_spark,
            source_path=source_path,
            target_table=target_table,
            file_format="JSON",
            table_schema=financial_schema,
            drop_table_if_exists=True,
            copy_options=copy_options
        )
        
        # Verify complete workflow
        expected_calls = [
            call("DROP TABLE IF EXISTS dev.financial.transactions_copy"),
            call("SHOW TABLES IN dev.financial LIKE 'transactions_copy'"),
            call("CREATE TABLE dev.financial.transactions_copy (transaction_id STRING NOT NULL, amount STRING, currency STRING, transaction_date STRING)"),
            call("COPY INTO dev.financial.transactions_copy FROM '/volumes/dev/financial/bronze/*.json' FILEFORMAT = JSON OPTIONS (multiline = 'true', timestampFormat = 'yyyy-MM-dd HH:mm:ss')"),
            call("SELECT * FROM dev.financial.transactions_copy")
        ]
        
        assert mock_spark.sql.call_count == 5
        mock_spark.sql.assert_has_calls(expected_calls)
        assert result == mock_result_df