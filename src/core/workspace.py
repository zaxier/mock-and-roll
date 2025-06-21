"""Databricks Workspace URL Generation

Utilities for generating Databricks workspace URLs to schema data locations.
"""

import os
import configparser
from pathlib import Path
from urllib.parse import urlparse

from config.settings import Config


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
    # Get profile name from environment, default to 'DEFAULT'  
    profile_name = os.getenv('DATABRICKS_CONFIG_PROFILE', 'DEFAULT')
    
    # Read ~/.databrickscfg
    databricks_cfg_path = Path.home() / '.databrickscfg'
    if not databricks_cfg_path.exists():
        raise FileNotFoundError(f"Databricks config file not found at {databricks_cfg_path}")
    
    config_parser = configparser.ConfigParser()
    config_parser.read(databricks_cfg_path)
    
    if profile_name not in config_parser:
        available_profiles = list(config_parser.sections())
        raise KeyError(f"Profile '{profile_name}' not found in ~/.databrickscfg. Available profiles: {available_profiles}")
    
    host = config_parser[profile_name].get('host')
    if not host:
        raise ValueError(f"No host found for profile '{profile_name}' in ~/.databrickscfg")
    
    # Extract workspace name from host URL
    parsed_url = urlparse(host)
    if not parsed_url.hostname:
        raise ValueError(f"Could not parse hostname from host URL: {host}")
    
    # Extract workspace name (first part before first dot)
    hostname_parts = parsed_url.hostname.split('.')
    if len(hostname_parts) < 2:
        raise ValueError(f"Host URL format not recognized: {host}")
    
    workspace_name = hostname_parts[0]
    
    # Get catalog and schema from config
    catalog = config.databricks.catalog
    schema = config.databricks.schema
    
    if not catalog or not schema:
        raise ValueError(f"Missing catalog ({catalog}) or schema ({schema}) in configuration")
    
    # Construct the full URL using the original hostname to preserve domain structure
    return f"https://{parsed_url.hostname}/explore/data/{catalog}/{schema}"


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