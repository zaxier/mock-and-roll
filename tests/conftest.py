"""Test configuration and shared fixtures."""

import pytest
from pathlib import Path


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Provide a temporary directory for test outputs."""
    return tmp_path
