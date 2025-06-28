"""Databricks Workspace URL Generation

Utilities for generating Databricks workspace URLs to schema data locations.
"""

import os
import configparser
from pathlib import Path
from urllib.parse import urlparse

from config.settings import Config
from core.logging_config import get_logger

logger = get_logger(__name__)


def get_workspace_schema_url(config: Config) -> str:
    """
    Generate a Databricks workspace URL to the schema that was just written to.
    
    Returns URL format: https://{workspace}.cloud.databricks.com/explore/data/{catalog}/{schema}
    
    Args:
        config: Configuration object containing catalog and schema information
        
    Returns:
        str: Complete URL to the Databricks workspace schema explorer
        
    Raises:
        FileNotFoundError: If ~/.databrickscfg is not found
        KeyError: If the specified profile is not found in the config
        ValueError: If host URL is missing or malformed
    """
    try:
        logger.debug("Attempting to generate Databricks workspace URL")
        
        # Get profile name from environment, default to 'DEFAULT'  
        profile_name = os.getenv('DATABRICKS_CONFIG_PROFILE', 'DEFAULT')
        logger.debug(f"Using Databricks profile: {profile_name}")
        
        # Read ~/.databrickscfg
        databricks_cfg_path = Path.home() / '.databrickscfg'
        if not databricks_cfg_path.exists():
            logger.error(f"Databricks config file not found at {databricks_cfg_path}")
            raise FileNotFoundError(f"Databricks config file not found at {databricks_cfg_path}")
        
        config_parser = configparser.ConfigParser()
        config_parser.read(databricks_cfg_path)
        
        if profile_name not in config_parser:
            available_profiles = list(config_parser.sections())
            logger.error(f"Profile '{profile_name}' not found in {databricks_cfg_path}. Available profiles: {available_profiles}")
            raise KeyError(f"Profile '{profile_name}' not found in ~/.databrickscfg. Available profiles: {available_profiles}")
        
        host = config_parser[profile_name].get('host')
        if not host:
            logger.error(f"No host found for profile '{profile_name}' in {databricks_cfg_path}")
            raise ValueError(f"No host found for profile '{profile_name}' in ~/.databrickscfg")
        
        # Extract workspace name from host URL
        parsed_url = urlparse(host)
        if not parsed_url.hostname:
            logger.error(f"Could not parse hostname from host URL: {host}")
            raise ValueError(f"Could not parse hostname from host URL: {host}")
        
        # Get catalog and schema from config
        catalog = config.databricks.catalog
        schema = config.databricks.schema
        
        if not catalog or not schema:
            logger.error(f"Missing catalog ('{catalog}') or schema ('{schema}') in configuration")
            raise ValueError(f"Missing catalog ({catalog}) or schema ({schema}) in configuration")
        
        # Construct the full URL
        workspace_url = f"https://{parsed_url.hostname}/explore/data/{catalog}/{schema}"
        logger.info(f"Successfully generated workspace URL: {workspace_url}")
        
        return workspace_url

    except Exception as e:
        logger.exception(f"An unexpected error occurred while generating workspace URL: {e}")
        raise


def get_workspace_info(config: Config) -> dict:
    """
    Get workspace connection information for debugging purposes.
    
    Args:
        config: Configuration object
        
    Returns:
        dict: Dictionary containing workspace connection details
    """
    try:
        profile_name = os.getenv('DATABRICKS_CONFIG_PROFILE', 'DEFAULT')
        
        databricks_cfg_path = Path.home() / '.databrickscfg'
        if not databricks_cfg_path.exists():
            return {"error": f"Config file not found: {databricks_cfg_path}"}
        
        config_parser = configparser.ConfigParser()
        config_parser.read(databricks_cfg_path)
        
        if profile_name not in config_parser:
            return {
                "error": f"Profile '{profile_name}' not found",
                "available_profiles": list(config_parser.sections())
            }
        
        host = config_parser[profile_name].get('host')
        parsed_url = urlparse(host) if host else None
        workspace_name = parsed_url.hostname.split('.')[0] if parsed_url and parsed_url.hostname else None
        
        return {
            "profile": profile_name,
            "host": host,
            "workspace_name": workspace_name,
            "catalog": config.databricks.catalog,
            "schema": config.databricks.schema,
            "config_file": str(databricks_cfg_path)
        }
        
    except Exception as e:
        return {"error": str(e)}