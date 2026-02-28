"""Databricks workspace URL generation."""

import configparser
import os
from pathlib import Path
from urllib.parse import urlparse

from .config import Config
from .logging_config import get_logger

logger = get_logger(__name__)


def get_workspace_schema_url(config: Config) -> str:
    """Generate a Databricks workspace URL to the schema explorer.

    Returns URL format: https://{host}/explore/data/{catalog}/{schema}
    """
    profile_name = config.profile or os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")

    databricks_cfg_path = Path.home() / ".databrickscfg"
    if not databricks_cfg_path.exists():
        raise FileNotFoundError(
            f"Databricks config file not found at {databricks_cfg_path}"
        )

    config_parser = configparser.ConfigParser()
    config_parser.read(databricks_cfg_path)

    if profile_name not in config_parser:
        available = list(config_parser.sections())
        raise KeyError(
            f"Profile '{profile_name}' not found. Available: {available}"
        )

    host = config_parser[profile_name].get("host")
    if not host:
        raise ValueError(f"No host found for profile '{profile_name}'")

    parsed = urlparse(host)
    if not parsed.hostname:
        raise ValueError(f"Could not parse hostname from: {host}")

    url = f"https://{parsed.hostname}/explore/data/{config.catalog}/{config.schema}"
    logger.info(f"Workspace URL: {url}")
    return url
