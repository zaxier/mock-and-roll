"""Build standalone wheel from a generated project."""

import subprocess
import sys
from pathlib import Path


def build_wheel(project_dir: Path, output_dir: Path | None = None) -> Path:
    """Build a wheel from a generated project.

    Args:
        project_dir: Root of the generated project (contains pyproject.toml).
        output_dir: Where to place the wheel. Defaults to project_dir/dist/.

    Returns:
        Path to the dist directory containing the wheel.
    """
    if not (project_dir / "pyproject.toml").exists():
        raise FileNotFoundError(
            f"No pyproject.toml found in {project_dir}. "
            "Are you in a mock-and-roll generated project?"
        )

    dist_dir = output_dir or (project_dir / "dist")
    dist_dir.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "build", "--wheel", "--outdir", str(dist_dir)]

    result = subprocess.run(
        cmd,
        cwd=str(project_dir),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Build failed (exit code {result.returncode}):\n{result.stderr}"
        )

    return dist_dir
