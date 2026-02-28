"""Project scaffolding: render templates and copy bundled_core."""

import shutil
from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape


def _get_jinja_env() -> Environment:
    return Environment(
        loader=PackageLoader("mock_and_roll", "templates"),
        autoescape=select_autoescape([]),
        keep_trailing_newline=True,
    )


def scaffold_project(
    project_name: str,
    output_dir: Path,
    catalog: str = "dev",
    schema: str = "default",
    records: int = 1000,
) -> Path:
    """Scaffold a new project directory with templates and bundled core.

    Args:
        project_name: Human-readable name (may contain hyphens).
        output_dir: Parent directory to create the project in.
        catalog: Default Databricks catalog.
        schema: Default Databricks schema.
        records: Default record count.

    Returns:
        Path to the created project directory.
    """
    package_name = project_name.replace("-", "_")
    project_dir = output_dir / project_name
    src_dir = project_dir / "src" / package_name

    # Create directories
    src_dir.mkdir(parents=True, exist_ok=True)

    env = _get_jinja_env()
    context = {
        "project_name": project_name,
        "package_name": package_name,
        "catalog": catalog,
        "schema": schema,
        "records": records,
    }

    # Render templates into project root
    _render_template(env, "pyproject.toml.j2", project_dir / "pyproject.toml", context)
    _render_template(env, "README.md.j2", project_dir / "README.md", context)
    _render_template(env, "CLAUDE.md.j2", project_dir / "CLAUDE.md", context)
    _render_template(env, ".env.example.j2", project_dir / ".env.example", context)
    _render_template(env, ".gitignore.j2", project_dir / ".gitignore", context)

    # Render templates into src/package_name/
    _render_template(env, "__init__.py.j2", src_dir / "__init__.py", context)
    _render_template(env, "__main__.py.j2", src_dir / "__main__.py", context)
    _render_template(env, "main.py.j2", src_dir / "main.py", context)
    _render_template(env, "datasets.py.j2", src_dir / "datasets.py", context)

    # Copy bundled_core into src/package_name/core/
    _copy_bundled_core(src_dir / "core")

    return project_dir


def _render_template(
    env: Environment, template_name: str, dest: Path, context: dict
) -> None:
    template = env.get_template(template_name)
    dest.write_text(template.render(**context))


def _copy_bundled_core(dest: Path) -> None:
    """Copy the bundled_core package into the destination as 'core'."""
    bundled_core_dir = Path(__file__).parent / "bundled_core"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(bundled_core_dir, dest)
