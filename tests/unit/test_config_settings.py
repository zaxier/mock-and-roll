"""
Tests for configuration settings module.
"""

import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open
import pytest

from src.config.settings import (
    DatabricksConfig,
    DataGenerationConfig,
    StorageConfig,
    LoggingConfig,
    AppConfig,
    Config,
    load_yaml_config,
    merge_configs,
    apply_env_overrides,
    load_dotenv_files,
    load_config,
    get_config
)


class TestDatabricksConfig:
    """Test DatabricksConfig dataclass."""
    
    def test_initialization(self):
        """Test basic initialization of DatabricksConfig."""
        config = DatabricksConfig(
            catalog="test_catalog",
            schema="test_schema", 
            volume="test_volume"
        )
        
        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"
        assert config.volume == "test_volume"
        assert config.auto_create_catalog == False  # Default value
        assert config.auto_create_schema == True    # Default value
        assert config.auto_create_volume == True    # Default value
    
    def test_get_volume_path_base(self):
        """Test getting base volume path without file path."""
        config = DatabricksConfig(
            catalog="dev",
            schema="demo",
            volume="data"
        )
        
        expected = "/Volumes/dev/demo/data"
        assert config.get_volume_path() == expected
    
    def test_get_volume_path_with_file(self):
        """Test getting volume path with file path."""
        config = DatabricksConfig(
            catalog="dev",
            schema="demo", 
            volume="data"
        )
        
        expected = "/Volumes/dev/demo/data/transactions/daily"
        assert config.get_volume_path("transactions/daily") == expected
    
    def test_get_volume_path_strips_leading_slash(self):
        """Test that leading slash is stripped from file path."""
        config = DatabricksConfig(
            catalog="dev",
            schema="demo",
            volume="data"
        )
        
        expected = "/Volumes/dev/demo/data/transactions/daily"
        assert config.get_volume_path("/transactions/daily") == expected
    
    def test_auto_create_initialization(self):
        """Test initialization with explicit auto_create values."""
        config = DatabricksConfig(
            catalog="test_catalog",
            schema="test_schema", 
            volume="test_volume",
            auto_create_catalog=True,
            auto_create_schema=False,
            auto_create_volume=False
        )
        
        assert config.auto_create_catalog == True
        assert config.auto_create_schema == False
        assert config.auto_create_volume == False


class TestDataGenerationConfig:
    """Test DataGenerationConfig dataclass."""
    
    def test_initialization(self):
        """Test basic initialization."""
        config = DataGenerationConfig(
            default_records=1000,
            date_range_days=365,
            batch_size=10000
        )
        
        assert config.default_records == 1000
        assert config.date_range_days == 365
        assert config.batch_size == 10000


class TestStorageConfig:
    """Test StorageConfig dataclass."""
    
    def test_initialization(self):
        """Test basic initialization."""
        config = StorageConfig(
            default_format="parquet",
            compression="snappy",
            write_mode="overwrite"
        )
        
        assert config.default_format == "parquet"
        assert config.compression == "snappy"
        assert config.write_mode == "overwrite"


class TestLoggingConfig:
    """Test LoggingConfig dataclass."""
    
    def test_initialization(self):
        """Test basic initialization."""
        config = LoggingConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        assert config.level == "INFO"
        assert "%(asctime)s" in config.format


class TestAppConfig:
    """Test AppConfig dataclass."""
    
    def test_initialization(self):
        """Test basic initialization."""
        config = AppConfig(
            name="test-app",
            version="1.0.0"
        )
        
        assert config.name == "test-app"
        assert config.version == "1.0.0"


class TestConfig:
    """Test main Config dataclass."""
    
    def test_initialization(self):
        """Test complete config initialization."""
        databricks = DatabricksConfig("dev", "schema", "volume")
        data_gen = DataGenerationConfig(1000, 365, 10000)
        storage = StorageConfig("parquet", "snappy", "overwrite")
        logging = LoggingConfig("INFO", "format")
        app = AppConfig("app", "1.0.0")
        
        config = Config(
            environment="dev",
            databricks=databricks,
            data_generation=data_gen,
            storage=storage,
            logging=logging,
            app=app
        )
        
        assert config.environment == "dev"
        assert config.databricks == databricks
        assert config.data_generation == data_gen
        assert config.storage == storage
        assert config.logging == logging
        assert config.app == app


class TestLoadYamlConfig:
    """Test YAML configuration loading."""
    
    def test_load_existing_file(self):
        """Test loading an existing YAML file."""
        yaml_content = {
            'databricks': {
                'catalog': 'test',
                'schema': 'test_schema',
                'volume': 'test_volume'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_path = Path(f.name)
        
        try:
            result = load_yaml_config(temp_path)
            assert result == yaml_content
        finally:
            temp_path.unlink()
    
    def test_load_nonexistent_file(self):
        """Test loading a non-existent file raises FileNotFoundError."""
        nonexistent_path = Path("/tmp/nonexistent_config.yml")
        
        with pytest.raises(FileNotFoundError) as exc_info:
            load_yaml_config(nonexistent_path)
        
        assert "Configuration file not found" in str(exc_info.value)


class TestMergeConfigs:
    """Test configuration merging functionality."""
    
    def test_merge_simple_configs(self):
        """Test merging non-nested configurations."""
        base = {'a': 1, 'b': 2}
        override = {'b': 3, 'c': 4}
        
        result = merge_configs(base, override)
        expected = {'a': 1, 'b': 3, 'c': 4}
        
        assert result == expected
    
    def test_merge_nested_configs(self):
        """Test merging nested configurations."""
        base = {
            'databricks': {
                'catalog': 'dev',
                'schema': 'old_schema'
            },
            'storage': {
                'format': 'parquet'
            }
        }
        
        override = {
            'databricks': {
                'schema': 'new_schema',
                'volume': 'new_volume'
            }
        }
        
        result = merge_configs(base, override)
        expected = {
            'databricks': {
                'catalog': 'dev',
                'schema': 'new_schema',
                'volume': 'new_volume'
            },
            'storage': {
                'format': 'parquet'
            }
        }
        
        assert result == expected
    
    def test_merge_preserves_original(self):
        """Test that merge doesn't modify original configs."""
        base = {'a': 1, 'b': 2}
        override = {'b': 3, 'c': 4}
        
        original_base = base.copy()
        original_override = override.copy()
        
        merge_configs(base, override)
        
        assert base == original_base
        assert override == original_override


class TestApplyEnvOverrides:
    """Test environment variable override functionality."""
    
    def test_apply_string_overrides(self):
        """Test applying string environment variable overrides."""
        config = {
            'databricks': {
                'catalog': 'dev',
                'schema': 'old_schema'
            }
        }
        
        with patch.dict(os.environ, {
            'DATABRICKS_CATALOG': 'prod',
            'DATABRICKS_SCHEMA': 'new_schema'
        }):
            result = apply_env_overrides(config)
        
        assert result['databricks']['catalog'] == 'prod'
        assert result['databricks']['schema'] == 'new_schema'
    
    def test_apply_integer_overrides(self):
        """Test applying integer environment variable overrides."""
        config = {
            'data_generation': {
                'default_records': 1000
            }
        }
        
        with patch.dict(os.environ, {'DATA_RECORDS': '5000'}):
            result = apply_env_overrides(config)
        
        assert result['data_generation']['default_records'] == 5000
        assert isinstance(result['data_generation']['default_records'], int)
    
    def test_apply_boolean_overrides(self):
        """Test applying boolean environment variable overrides."""
        config = {
            'databricks': {
                'auto_create_catalog': False,
                'auto_create_schema': True,
                'auto_create_volume': False
            }
        }
        
        with patch.dict(os.environ, {
            'DATABRICKS_AUTO_CREATE_CATALOG': 'true',
            'DATABRICKS_AUTO_CREATE_SCHEMA': 'false',
            'DATABRICKS_AUTO_CREATE_VOLUME': 'true'
        }):
            result = apply_env_overrides(config)
        
        assert result['databricks']['auto_create_catalog'] == True
        assert result['databricks']['auto_create_schema'] == False
        assert result['databricks']['auto_create_volume'] == True
        assert isinstance(result['databricks']['auto_create_catalog'], bool)
        assert isinstance(result['databricks']['auto_create_schema'], bool)
        assert isinstance(result['databricks']['auto_create_volume'], bool)
    
    def test_boolean_env_var_parsing(self):
        """Test various boolean string representations."""
        config = {'databricks': {'auto_create_volume': False}}
        
        # Test truthy values
        for truthy_value in ['true', 'True', '1', 'yes', 'on']:
            with patch.dict(os.environ, {'DATABRICKS_AUTO_CREATE_VOLUME': truthy_value}):
                result = apply_env_overrides(config)
                assert result['databricks']['auto_create_volume'] == True
        
        # Test falsy values
        for falsy_value in ['false', 'False', '0', 'no', 'off', 'anything_else']:
            with patch.dict(os.environ, {'DATABRICKS_AUTO_CREATE_VOLUME': falsy_value}):
                result = apply_env_overrides(config)
                assert result['databricks']['auto_create_volume'] == False
    
    def test_no_env_vars_no_change(self):
        """Test that config is unchanged when no env vars are set."""
        config = {
            'databricks': {
                'catalog': 'dev'
            }
        }
        
        # Clear any existing env vars
        env_vars_to_clear = [
            'DATABRICKS_CATALOG', 'DATABRICKS_SCHEMA', 'DATABRICKS_VOLUME',
            'DATABRICKS_AUTO_CREATE_VOLUME', 'DATA_RECORDS', 'STORAGE_FORMAT', 'LOG_LEVEL'
        ]
        
        with patch.dict(os.environ, {}, clear=True):
            result = apply_env_overrides(config)
        
        assert result == config
    
    def test_creates_missing_nested_keys(self):
        """Test that missing nested keys are created."""
        config = {}
        
        with patch.dict(os.environ, {'DATABRICKS_CATALOG': 'test'}):
            result = apply_env_overrides(config)
        
        assert result['databricks']['catalog'] == 'test'


class TestLoadDotenvFiles:
    """Test .env file loading functionality."""
    
    @patch('src.config.settings.load_dotenv')
    @patch('pathlib.Path.exists')
    def test_load_dotenv_files_both_exist(self, mock_exists, mock_load_dotenv):
        """Test loading when both .env and .env.local exist."""
        mock_exists.return_value = True
        project_root = Path("/fake/project")
        
        load_dotenv_files(project_root)
        
        # Should be called twice, once for each file
        assert mock_load_dotenv.call_count == 2
        
        # Check the files were loaded in correct order
        expected_calls = [
            ((project_root / '.env',), {'override': True}),
            ((project_root / '.env.local',), {'override': True})
        ]
        actual_calls = [(call.args, call.kwargs) for call in mock_load_dotenv.call_args_list]
        assert actual_calls == expected_calls
    
    @patch('src.config.settings.load_dotenv')
    @patch('pathlib.Path.exists')
    def test_load_dotenv_files_only_env_exists(self, mock_exists, mock_load_dotenv):
        """Test loading when only .env exists."""
        def exists_side_effect():
            # Get the path from the mock call history
            path = mock_exists.call_args[0][0] if mock_exists.call_args else ""
            return str(path).endswith('.env') and not str(path).endswith('.env.local')
        
        # Use explicit return values instead of side_effect for simplicity
        # First call for .env returns True, second call for .env.local returns False
        mock_exists.side_effect = [True, False]
        project_root = Path("/fake/project")
        
        load_dotenv_files(project_root)
        
        # Should be called only once for .env
        assert mock_load_dotenv.call_count == 1
        mock_load_dotenv.assert_called_with(project_root / '.env', override=True)
    
    @patch('src.config.settings.load_dotenv')
    @patch('pathlib.Path.exists')
    def test_load_dotenv_files_none_exist(self, mock_exists, mock_load_dotenv):
        """Test loading when no .env files exist."""
        mock_exists.return_value = False
        project_root = Path("/fake/project")
        
        load_dotenv_files(project_root)
        
        # Should not be called at all
        assert mock_load_dotenv.call_count == 0


class TestLoadConfig:
    """Test full configuration loading."""
    
    @patch('src.config.settings.load_dotenv_files')
    @patch('src.config.settings.load_yaml_config')
    def test_load_config_default_environment(self, mock_load_yaml, mock_load_dotenv):
        """Test loading config with default environment."""
        mock_config = {
            'databricks': {'catalog': 'dev', 'schema': 'test', 'volume': 'data', 'auto_create_catalog': False, 'auto_create_schema': True, 'auto_create_volume': True},
            'data_generation': {'default_records': 1000, 'date_range_days': 365, 'batch_size': 10000},
            'storage': {'default_format': 'parquet', 'compression': 'snappy', 'write_mode': 'overwrite'},
            'logging': {'level': 'INFO', 'format': 'test-format'},
            'app': {'name': 'test-app', 'version': '1.0.0'}
        }
        
        mock_load_yaml.return_value = mock_config
        mock_load_dotenv.return_value = None  # Don't load any .env files
        
        with patch.dict(os.environ, {}, clear=True):
            config = load_config()
        
        assert isinstance(config, Config)
        assert config.databricks.catalog == 'dev'
        assert config.data_generation.default_records == 1000
        assert config.databricks.auto_create_catalog == False
        assert config.databricks.auto_create_schema == True
        assert config.databricks.auto_create_volume == True
    
    @patch('src.config.settings.load_dotenv_files')
    @patch('src.config.settings.load_yaml_config')
    def test_load_config_with_environment(self, mock_load_yaml, mock_load_dotenv):
        """Test loading config with specific environment."""
        base_config = {
            'databricks': {'catalog': 'dev', 'schema': 'test', 'volume': 'data', 'auto_create_catalog': False, 'auto_create_schema': True, 'auto_create_volume': True},
            'data_generation': {'default_records': 1000, 'date_range_days': 365, 'batch_size': 10000},
            'storage': {'default_format': 'parquet', 'compression': 'snappy', 'write_mode': 'overwrite'},
            'logging': {'level': 'INFO', 'format': 'test-format'},
            'app': {'name': 'test-app', 'version': '1.0.0'}
        }
        
        env_config = {
            'databricks': {'catalog': 'prod'}
        }
        
        mock_load_yaml.side_effect = [base_config, env_config]
        mock_load_dotenv.return_value = None  # Don't load any .env files
        
        with patch('pathlib.Path.exists', return_value=True), patch.dict(os.environ, {}, clear=True):
            config = load_config('prod')
        
        assert config.databricks.catalog == 'prod'
        assert config.databricks.schema == 'test'  # From base config
        assert config.databricks.auto_create_catalog == False  # From base config
        assert config.databricks.auto_create_schema == True   # From base config
        assert config.databricks.auto_create_volume == True   # From base config


class TestGetConfig:
    """Test get_config singleton functionality."""
    
    def test_get_config_singleton(self):
        """Test that get_config returns the same instance."""
        # Reset global config
        import src.config.settings
        src.config.settings._config = None
        
        with patch('src.config.settings.load_config') as mock_load:
            mock_config = Config(
                environment='dev',
                databricks=DatabricksConfig('dev', 'test', 'data'),
                data_generation=DataGenerationConfig(1000, 365, 10000),
                storage=StorageConfig('parquet', 'snappy', 'overwrite'),
                logging=LoggingConfig('INFO', 'format'),
                app=AppConfig('app', '1.0.0')
            )
            mock_load.return_value = mock_config
            
            config1 = get_config()
            config2 = get_config()
            
            assert config1 is config2
            assert mock_load.call_count == 1
    
    def test_get_config_reload(self):
        """Test that reload forces new config load."""
        # Reset global config
        import src.config.settings
        src.config.settings._config = None
        
        with patch('src.config.settings.load_config') as mock_load:
            mock_config = Config(
                environment='dev',
                databricks=DatabricksConfig('dev', 'test', 'data'),
                data_generation=DataGenerationConfig(1000, 365, 10000),
                storage=StorageConfig('parquet', 'snappy', 'overwrite'),
                logging=LoggingConfig('INFO', 'format'),
                app=AppConfig('app', '1.0.0')
            )
            mock_load.return_value = mock_config
            
            config1 = get_config()
            config2 = get_config(reload=True)
            
            assert mock_load.call_count == 2