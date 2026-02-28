"""Tests for bundled_core config module."""

import os
from unittest.mock import patch

from mock_and_roll.bundled_core.config import Config, get_config, _apply_env_vars


def test_config_defaults():
    config = Config()
    assert config.catalog == "dev"
    assert config.schema == "default"
    assert config.volume == "sample_data"
    assert config.records == 1000
    assert config.log_level == "INFO"
    assert config.profile == "DEFAULT"
    assert config.auto_create_schema is True
    assert config.auto_create_volume is True


def test_config_get_volume_path():
    config = Config(catalog="my_cat", schema="my_schema", volume="my_vol")
    assert config.get_volume_path() == "/Volumes/my_cat/my_schema/my_vol"
    assert (
        config.get_volume_path("raw/data.parquet")
        == "/Volumes/my_cat/my_schema/my_vol/raw/data.parquet"
    )
    # Leading slash should be stripped
    assert (
        config.get_volume_path("/raw/data.parquet")
        == "/Volumes/my_cat/my_schema/my_vol/raw/data.parquet"
    )


def test_apply_env_vars():
    config = Config()
    with patch.dict(
        os.environ,
        {
            "DATABRICKS_CATALOG": "env_catalog",
            "DATABRICKS_SCHEMA": "env_schema",
            "DATA_RECORDS": "5000",
        },
    ):
        config = _apply_env_vars(config)
    assert config.catalog == "env_catalog"
    assert config.schema == "env_schema"
    assert config.records == 5000


def test_get_config_with_cli_overrides():
    with patch("sys.argv", ["prog"]):
        config = get_config(cli_overrides={"catalog": "cli_cat", "records": 200})
    assert config.catalog == "cli_cat"
    assert config.records == 200


def test_get_config_cli_overrides_take_precedence():
    with patch("sys.argv", ["prog"]):
        with patch.dict(os.environ, {"DATABRICKS_CATALOG": "env_cat"}):
            config = get_config(cli_overrides={"catalog": "cli_cat"})
    assert config.catalog == "cli_cat"
