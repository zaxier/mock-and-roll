"""Tests for the builder module."""

import pytest
from pathlib import Path

from mock_and_roll.builder import build_wheel


def test_build_wheel_missing_pyproject(tmp_path):
    """Build should fail if no pyproject.toml exists."""
    with pytest.raises(FileNotFoundError, match="pyproject.toml"):
        build_wheel(tmp_path)


def test_build_wheel_creates_dist_dir(tmp_path):
    """Build should create the output dist directory."""
    from mock_and_roll.scaffold import scaffold_project

    project_dir = scaffold_project("test_build_project", tmp_path)
    dist_dir = build_wheel(project_dir)

    assert dist_dir.exists()
    whl_files = list(dist_dir.glob("*.whl"))
    assert len(whl_files) == 1
    assert "test_build_project" in whl_files[0].name
