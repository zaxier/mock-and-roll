"""Integration test: scaffold a project and build a wheel."""

from pathlib import Path

from click.testing import CliRunner

from mock_and_roll.cli import main


def test_scaffold_and_build(tmp_path):
    """Create a project, then build a wheel from it."""
    runner = CliRunner()

    # Step 1: Create
    result = runner.invoke(
        main,
        ["create", "build_test_demo", "--no-ai", "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0

    project_dir = tmp_path / "build_test_demo"

    # Step 2: Build
    result = runner.invoke(
        main,
        ["build", "--project-dir", str(project_dir)],
    )
    assert result.exit_code == 0
    assert "Wheel built successfully" in result.output

    # Verify wheel exists
    dist_dir = project_dir / "dist"
    whl_files = list(dist_dir.glob("*.whl"))
    assert len(whl_files) == 1
    assert "build_test_demo" in whl_files[0].name
