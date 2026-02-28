"""Simplified configuration for generated projects.

Resolution order: defaults -> env vars -> CLI args. No YAML, no .env loading.
"""

import argparse
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Config:
    """Pipeline configuration."""

    catalog: str = "dev"
    schema: str = "default"
    volume: str = "sample_data"
    records: int = 1000
    log_level: str = "INFO"
    profile: str = "DEFAULT"
    auto_create_schema: bool = True
    auto_create_volume: bool = True

    def get_volume_path(self, file_path: str = "") -> str:
        base = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        return f"{base}/{file_path.lstrip('/')}" if file_path else base


def _apply_env_vars(config: Config) -> Config:
    """Apply environment variable overrides."""
    env_map = {
        "DATABRICKS_CATALOG": "catalog",
        "DATABRICKS_SCHEMA": "schema",
        "DATABRICKS_VOLUME": "volume",
        "DATABRICKS_CONFIG_PROFILE": "profile",
        "DATA_RECORDS": "records",
        "LOG_LEVEL": "log_level",
    }
    for env_var, attr in env_map.items():
        val = os.getenv(env_var)
        if val is not None:
            if attr == "records":
                setattr(config, attr, int(val))
            elif attr in ("auto_create_schema", "auto_create_volume"):
                setattr(config, attr, val.lower() in ("true", "1", "yes", "on"))
            else:
                setattr(config, attr, val)
    return config


def parse_args(
    description: str = "Demo Pipeline",
    custom_args: Optional[List[Tuple[str, type, str]]] = None,
) -> Dict[str, Any]:
    """Parse CLI arguments. Returns dict with non-None values only."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--schema", type=str, help="Databricks schema name")
    parser.add_argument("--catalog", type=str, help="Databricks catalog name")
    parser.add_argument("--volume", type=str, help="Databricks volume name")
    parser.add_argument("--records", type=int, help="Number of records to generate")
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    if custom_args:
        for arg_name, arg_type, help_text in custom_args:
            parser.add_argument(arg_name, type=arg_type, help=help_text)

    args = parser.parse_args()
    return {k: v for k, v in vars(args).items() if v is not None}


def get_config(
    cli_overrides: Optional[Dict[str, Any]] = None,
) -> Config:
    """Build config: defaults -> env vars -> CLI args."""
    config = Config()
    config = _apply_env_vars(config)

    if cli_overrides:
        cli_map = {
            "catalog": "catalog",
            "schema": "schema",
            "volume": "volume",
            "records": "records",
            "log_level": "log_level",
        }
        for cli_key, attr in cli_map.items():
            val = cli_overrides.get(cli_key)
            if val is not None:
                setattr(config, attr, val)

    return config
