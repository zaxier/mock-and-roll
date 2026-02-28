"""Tests for project scaffolding."""

from pathlib import Path

from mock_and_roll.scaffold import scaffold_project


def test_scaffold_creates_project(tmp_path):
    project_dir = scaffold_project("test_project", tmp_path)

    assert project_dir == tmp_path / "test_project"
    assert project_dir.exists()


def test_scaffold_creates_all_files(tmp_path):
    project_dir = scaffold_project("test_project", tmp_path)

    # Root files
    assert (project_dir / "pyproject.toml").exists()
    assert (project_dir / "README.md").exists()
    assert (project_dir / "CLAUDE.md").exists()
    assert (project_dir / ".env.example").exists()
    assert (project_dir / ".gitignore").exists()

    # Source files
    src = project_dir / "src" / "test_project"
    assert (src / "__init__.py").exists()
    assert (src / "__main__.py").exists()
    assert (src / "main.py").exists()
    assert (src / "datasets.py").exists()

    # Core files
    core = src / "core"
    assert (core / "__init__.py").exists()
    assert (core / "config.py").exists()
    assert (core / "spark.py").exists()
    assert (core / "catalog.py").exists()
    assert (core / "io.py").exists()
    assert (core / "data.py").exists()
    assert (core / "logging_config.py").exists()
    assert (core / "workspace.py").exists()


def test_scaffold_renders_templates(tmp_path):
    project_dir = scaffold_project("my_demo", tmp_path)

    # Check pyproject.toml has project name
    pyproject = (project_dir / "pyproject.toml").read_text()
    assert 'name = "my_demo"' in pyproject

    # Check __main__.py references correct package
    main_py = (project_dir / "src" / "my_demo" / "__main__.py").read_text()
    assert "from .main import main" in main_py

    # Check CLAUDE.md has project name
    claude_md = (project_dir / "CLAUDE.md").read_text()
    assert "my_demo" in claude_md


def test_scaffold_hyphen_to_underscore(tmp_path):
    project_dir = scaffold_project("my-retail-demo", tmp_path)

    # Project dir keeps hyphens
    assert project_dir.name == "my-retail-demo"
    # Package name uses underscores
    assert (project_dir / "src" / "my_retail_demo" / "__init__.py").exists()

    # pyproject.toml has correct package name
    pyproject = (project_dir / "pyproject.toml").read_text()
    assert "my_retail_demo" in pyproject


def test_scaffold_core_is_valid_python(tmp_path):
    """Verify core modules have no syntax errors."""
    import py_compile

    project_dir = scaffold_project("test_project", tmp_path)
    core_dir = project_dir / "src" / "test_project" / "core"

    for py_file in core_dir.glob("*.py"):
        py_compile.compile(str(py_file), doraise=True)


def test_scaffold_templates_are_valid_python(tmp_path):
    """Verify rendered source files have no syntax errors."""
    import py_compile

    project_dir = scaffold_project("test_project", tmp_path)
    src_dir = project_dir / "src" / "test_project"

    for py_file in src_dir.glob("*.py"):
        py_compile.compile(str(py_file), doraise=True)
