"""
Configuration management for netsuite-aibi demo.
Supports YAML configuration files with environment variable overrides.
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any
from dotenv import load_dotenv
# Import logging directly to avoid circular import with core module
import logging

# Configure logging - use standard logging since core.logging_config depends on config
logger = logging.getLogger(__name__)




@dataclass
class DatabricksConfig:
    """Databricks connection and storage configuration."""
    catalog: str
    schema: str
    volume: str
    auto_create_catalog: bool = False
    auto_create_schema: bool = True
    auto_create_volume: bool = True
    
    def get_volume_path(self, file_path: str = "") -> str:
        """Get full volume path for storing data."""
        base_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        if file_path:
            return f"{base_path}/{file_path.lstrip('/')}"
        return base_path


@dataclass
class DataGenerationConfig:
    """Data generation parameters."""
    default_records: int
    date_range_days: int
    batch_size: int


@dataclass
class StorageConfig:
    """Storage configuration."""
    write_mode: str


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str
    format: str


@dataclass
class AppConfig:
    """Main application configuration."""
    name: str
    version: str


@dataclass
class Config:
    """Complete configuration object."""
    environment: str
    databricks: DatabricksConfig
    data_generation: DataGenerationConfig
    storage: StorageConfig
    logging: LoggingConfig
    app: AppConfig


def get_relative_path(path: Path, project_root: Optional[Path] = None) -> str:
    """Convert absolute path to relative path from project root."""
    try:
        if project_root is None:
            project_root = find_project_root()
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)


def load_yaml_config(config_path: Path, project_root: Optional[Path] = None) -> Dict[str, Any]:
    relative_path = get_relative_path(config_path, project_root)
    """Load YAML configuration file."""
    if not config_path.exists():
        logger.error(f"Configuration file not found: {relative_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    logger.debug(f"Loading configuration from: {relative_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    logger.debug(f"Successfully loaded configuration from {relative_path}")
    return config


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two configuration dictionaries, with override taking precedence."""
    result = base_config.copy()
    
    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
    
    return result


def load_dotenv_files(project_root: Path) -> None:
    """
    Load .env files in precedence order.
    
    Precedence (lowest to highest):
    1. .env - Team defaults (should be committed)
    2. .env.local - Personal overrides (should be gitignored)
    
    Args:
        project_root: Project root directory path
    """
    # Load .env files in precedence order
    env_files = [
        project_root / '.env',        # Team defaults (lower priority)
        project_root / '.env.local',  # Personal overrides (higher priority)
    ]
    
    for env_file in env_files:
        if env_file.exists():
            load_dotenv(env_file, override=True)
            logger.debug(f"Loaded environment variables from: {get_relative_path(env_file, project_root)}")
        else:
            logger.debug(f"No environment file found at: {get_relative_path(env_file, project_root)}")


def apply_cli_overrides(config: Dict[str, Any], cli_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Apply command line argument overrides to configuration."""
    if not cli_overrides:
        return config
        
    logger.debug(f"Applying CLI argument overrides")
    
    # Define CLI argument mappings
    cli_mappings = {
        'catalog': ['databricks', 'catalog'],
        'schema': ['databricks', 'schema'],
        'volume': ['databricks', 'volume'],
        'records': ['data_generation', 'default_records'],
        'log_level': ['logging', 'level'],
    }
    
    result = config.copy()
    overrides_applied = 0
    
    for cli_arg, config_path in cli_mappings.items():
        cli_value = cli_overrides.get(cli_arg)
        if cli_value is not None:
            # Navigate to the nested config location
            current = result
            for key in config_path[:-1]:
                current = current.setdefault(key, {})
            
            # Convert value to appropriate type
            final_key = config_path[-1]
            if final_key in ['default_records', 'date_range_days', 'batch_size']:
                current[final_key] = int(cli_value)
            else:
                current[final_key] = cli_value
            
            overrides_applied += 1
            logger.debug(f"Applied CLI override: --{cli_arg}={cli_value}")
    
    logger.debug(f"Applied {overrides_applied} CLI argument overrides")
    return result


def apply_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply environment variable overrides to configuration."""
    logger.debug(f"Applying environment variable overrides")
    
    # Define environment variable mappings
    env_mappings = {
        'DATABRICKS_CATALOG': ['databricks', 'catalog'],
        'DATABRICKS_SCHEMA': ['databricks', 'schema'],
        'DATABRICKS_VOLUME': ['databricks', 'volume'],
        'DATABRICKS_AUTO_CREATE_CATALOG': ['databricks', 'auto_create_catalog'],
        'DATABRICKS_AUTO_CREATE_SCHEMA': ['databricks', 'auto_create_schema'],
        'DATABRICKS_AUTO_CREATE_VOLUME': ['databricks', 'auto_create_volume'],
        'DATA_RECORDS': ['data_generation', 'default_records'],
        'LOG_LEVEL': ['logging', 'level'],
    }
    
    result = config.copy()
    overrides_applied = 0
    
    for env_var, config_path in env_mappings.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            # Navigate to the nested config location
            current = result
            for key in config_path[:-1]:
                current = current.setdefault(key, {})
            
            # Convert value to appropriate type
            final_key = config_path[-1]
            if final_key in ['default_records', 'date_range_days', 'batch_size']:
                current[final_key] = int(env_value)
            elif final_key in ['auto_create_catalog', 'auto_create_schema', 'auto_create_volume']:
                current[final_key] = env_value.lower() in ('true', '1', 'yes', 'on')
            else:
                current[final_key] = env_value
            
            overrides_applied += 1
            logger.debug(f"Applied override: {env_var}={env_value}")
    
    logger.debug(f"Applied {overrides_applied} environment variable overrides")
    return result


def find_project_root() -> Path:
    """Find project root by looking for marker files."""
    logger.debug(f"Searching for project root directory")
    current = Path.cwd()
    markers = ['pyproject.toml', 'databricks.yml', '.git']
    
    while current != current.parent:
        if any((current / marker).exists() for marker in markers):
            # Use absolute path here to avoid recursion
            logger.debug(f"Found project root at: {current}")
            return current
        current = current.parent
    
    logger.error(f"Could not find project root directory")
    raise FileNotFoundError("Could not find project root")


def load_config(environment: Optional[str] = None, cli_overrides: Optional[Dict[str, Any]] = None) -> Config:
    """
    Load configuration with full precedence chain.
    
    Precedence (lowest to highest priority):
    1. config/base.yml
    2. config/environments/{environment}.yml 
    3. .env (team defaults)
    4. .env.local (personal overrides)
    5. Environment variables (runtime/deployment)
    6. CLI arguments (highest priority)
    
    Args:
        environment: Environment name (dev, prod, etc.). If None, uses ENVIRONMENT env var or 'dev'
        cli_overrides: Dictionary of CLI argument overrides
    
    Returns:
        Complete configuration object
    """
    logger.info(f"Starting configuration load process")
    
    # Get project root directory
    project_root = find_project_root()
    config_dir = project_root / 'config'
    
    # Load .env files before determining environment (in case ENVIRONMENT is set in .env)
    load_dotenv_files(project_root)
    
    # Determine environment (may now be set by .env files)
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'dev')
    logger.info(f"Using environment: {environment}")
    
    # Load base configuration
    base_config_path = config_dir / 'base.yml'
    config_data = load_yaml_config(base_config_path, project_root)
    
    # Load environment-specific configuration if it exists
    env_config_path = config_dir / 'environments' / f'{environment}.yml'
    if env_config_path.exists():
        logger.debug(f"Loading environment-specific configuration from: {get_relative_path(env_config_path, project_root)}")
        env_config = load_yaml_config(env_config_path, project_root)
        config_data = merge_configs(config_data, env_config)
        logger.debug(f"Merged environment configuration with base configuration")
    else:
        logger.warning(f"No environment-specific configuration found at: {get_relative_path(env_config_path, project_root)}")
    
    # Apply environment variable overrides
    config_data = apply_env_overrides(config_data)
    
    # Apply CLI argument overrides (highest priority)
    config_data = apply_cli_overrides(config_data, cli_overrides)
    
    # Create typed configuration objects
    logger.info(f"Configuration loaded successfully")
    return Config(
        environment=environment,
        databricks=DatabricksConfig(**config_data['databricks']),
        data_generation=DataGenerationConfig(**config_data['data_generation']),
        storage=StorageConfig(**config_data['storage']),
        logging=LoggingConfig(**config_data['logging']),
        app=AppConfig(**config_data['app'])
    )


# Global configuration instance
_config: Optional[Config] = None


def get_config(environment: Optional[str] = None, reload: bool = False, cli_overrides: Optional[Dict[str, Any]] = None) -> Config:
    """
    Get configuration instance (singleton pattern).
    
    Args:
        environment: Environment name to load
        reload: Force reload of configuration
        cli_overrides: Dictionary of CLI argument overrides
        
    Returns:
        Configuration object
    """
    global _config
    
    if _config is None or reload or cli_overrides:
        logger.debug(f"{'Reloading' if reload else 'Loading'} configuration")
        _config = load_config(environment, cli_overrides)
    else:
        logger.debug(f"Using cached configuration")
    
    return _config