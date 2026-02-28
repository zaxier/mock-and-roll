"""Tests for the CLI commands."""

from click.testing import CliRunner

from mock_and_roll.cli import main


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "1.0.0" in result.output


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "create" in result.output
    assert "build" in result.output
    assert "design" in result.output
    assert "run" in result.output


def test_create_help():
    runner = CliRunner()
    result = runner.invoke(main, ["create", "--help"])
    assert result.exit_code == 0
    assert "--no-ai" in result.output
    assert "--catalog" in result.output
    assert "--schema" in result.output
    assert "--records" in result.output
    assert "--output-dir" in result.output


def test_create_scaffold(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["create", "test_demo", "--no-ai", "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert "Project created" in result.output

    project_dir = tmp_path / "test_demo"
    assert project_dir.exists()
    assert (project_dir / "pyproject.toml").exists()
    assert (project_dir / "src" / "test_demo" / "main.py").exists()
    assert (project_dir / "src" / "test_demo" / "datasets.py").exists()
    assert (project_dir / "src" / "test_demo" / "core" / "__init__.py").exists()


def test_create_hyphenated_name(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["create", "my-retail-demo", "--no-ai", "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0

    project_dir = tmp_path / "my-retail-demo"
    assert project_dir.exists()
    # Package name should have underscores
    assert (project_dir / "src" / "my_retail_demo" / "__init__.py").exists()


def test_build_no_pyproject(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, ["build", "--project-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "Error" in result.output or "pyproject.toml" in result.output
