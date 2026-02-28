"""mock-and-roll CLI: create, build, design, run."""

import subprocess
import sys
from pathlib import Path

import click

from . import __version__


@click.group()
@click.version_option(version=__version__, prog_name="mock-and-roll")
def main():
    """AI-powered synthetic data pipeline generator for Databricks."""


@main.command()
@click.argument("project_name")
@click.option("--no-ai", is_flag=True, help="Skip launching a coding agent after scaffolding.")
@click.option("--agent", default=None, type=str, help="Coding agent CLI command (default: claude). Env: MOCK_AND_ROLL_AGENT.")
@click.option("--catalog", default="dev", show_default=True, help="Default Databricks catalog.")
@click.option("--schema", default="default", show_default=True, help="Default Databricks schema.")
@click.option("--records", default=1000, show_default=True, type=int, help="Default record count.")
@click.option("--output-dir", default=".", show_default=True, type=click.Path(), help="Parent directory for the project.")
def create(project_name, no_ai, agent, catalog, schema, records, output_dir):
    """Scaffold a new synthetic data project.

    PROJECT_NAME is the name for your new project (e.g. my_retail_demo).
    """
    from .scaffold import scaffold_project
    from .ai_session import resolve_agent

    output_path = Path(output_dir).resolve()
    click.echo(f"Creating project '{project_name}' in {output_path}/")

    project_dir = scaffold_project(
        project_name=project_name,
        output_dir=output_path,
        catalog=catalog,
        schema=schema,
        records=records,
    )

    click.echo(f"Project created at {project_dir}")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  cd {project_name}")

    if no_ai:
        click.echo(f"  Edit src/{project_name.replace('-', '_')}/datasets.py")
        click.echo(f"  python -m {project_name.replace('-', '_')} --help")
    else:
        agent_name = resolve_agent(agent)
        click.echo(f"  Launching {agent_name} for AI-assisted dataset design...")
        click.echo()
        try:
            from .ai_session import launch_agent
            launch_agent(project_dir, agent)
        except FileNotFoundError as e:
            click.echo(f"Warning: {e}", err=True)
            click.echo(f"You can edit src/{project_name.replace('-', '_')}/datasets.py manually.")


@main.command()
@click.option("--project-dir", default=".", show_default=True, type=click.Path(exists=True), help="Project directory.")
@click.option("--output-dir", default=None, type=click.Path(), help="Output directory for wheel (default: ./dist/).")
def build(project_dir, output_dir):
    """Build a standalone wheel from a generated project."""
    from .builder import build_wheel

    project_path = Path(project_dir).resolve()
    output_path = Path(output_dir).resolve() if output_dir else None

    click.echo(f"Building wheel from {project_path}")

    try:
        dist_dir = build_wheel(project_path, output_path)
        click.echo(f"Wheel built successfully in {dist_dir}")
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"Build failed: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option("--project-dir", default=".", show_default=True, type=click.Path(exists=True), help="Project directory.")
@click.option("--agent", default=None, type=str, help="Coding agent CLI command (default: claude). Env: MOCK_AND_ROLL_AGENT.")
def design(project_dir, agent):
    """Launch a coding agent to iterate on dataset design."""
    from .ai_session import launch_agent, resolve_agent

    project_path = Path(project_dir).resolve()
    agent_name = resolve_agent(agent)
    click.echo(f"Opening {agent_name} in {project_path}")

    try:
        launch_agent(project_path, agent)
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option("--project-dir", default=".", show_default=True, type=click.Path(exists=True), help="Project directory.")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def run(project_dir, args):
    """Run a generated project locally without building a wheel."""
    project_path = Path(project_dir).resolve()

    # Determine package name from pyproject.toml or directory structure
    src_dir = project_path / "src"
    if not src_dir.exists():
        click.echo("Error: No src/ directory found. Is this a mock-and-roll project?", err=True)
        sys.exit(1)

    # Find the package directory (first non-__pycache__ dir in src/)
    package_dirs = [
        d for d in src_dir.iterdir()
        if d.is_dir() and d.name != "__pycache__"
    ]
    if not package_dirs:
        click.echo("Error: No package found in src/", err=True)
        sys.exit(1)

    package_name = package_dirs[0].name

    cmd = [sys.executable, "-m", package_name, *args]
    env_addition = {"PYTHONPATH": str(src_dir)}

    import os
    env = {**os.environ, **env_addition}

    result = subprocess.run(cmd, cwd=str(project_path), env=env)
    sys.exit(result.returncode)
