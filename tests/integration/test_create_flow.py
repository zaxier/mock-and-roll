"""Integration test: scaffold a project and verify the complete structure."""

import py_compile
from pathlib import Path

from click.testing import CliRunner

from mock_and_roll.cli import main


def test_full_create_flow(tmp_path):
    """Create a project and verify all files exist and are valid Python."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "create",
            "integration_test_demo",
            "--no-ai",
            "--output-dir",
            str(tmp_path),
            "--catalog",
            "test_catalog",
            "--schema",
            "test_schema",
        ],
    )
    assert result.exit_code == 0

    project_dir = tmp_path / "integration_test_demo"
    src_dir = project_dir / "src" / "integration_test_demo"
    core_dir = src_dir / "core"

    # All expected files
    expected_files = [
        project_dir / "pyproject.toml",
        project_dir / "README.md",
        project_dir / "CLAUDE.md",
        project_dir / ".env.example",
        project_dir / ".gitignore",
        src_dir / "__init__.py",
        src_dir / "__main__.py",
        src_dir / "main.py",
        src_dir / "datasets.py",
        core_dir / "__init__.py",
        core_dir / "config.py",
        core_dir / "spark.py",
        core_dir / "catalog.py",
        core_dir / "io.py",
        core_dir / "data.py",
        core_dir / "logging_config.py",
        core_dir / "workspace.py",
    ]

    for f in expected_files:
        assert f.exists(), f"Missing: {f}"

    # All Python files should compile
    for py_file in src_dir.rglob("*.py"):
        py_compile.compile(str(py_file), doraise=True)

    # pyproject.toml content check
    pyproject = (project_dir / "pyproject.toml").read_text()
    assert 'name = "integration_test_demo"' in pyproject
    assert "databricks-connect" in pyproject
    assert "mimesis" in pyproject
