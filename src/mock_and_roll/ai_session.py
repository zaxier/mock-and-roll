"""Launch a coding agent subprocess for AI-assisted dataset design."""

import os
import shutil
import subprocess
from pathlib import Path

DEFAULT_AGENT = "claude"
AGENT_ENV_VAR = "MOCK_AND_ROLL_AGENT"


def resolve_agent(agent: str | None = None) -> str:
    """Resolve the agent command: explicit arg > env var > default ('claude')."""
    return agent or os.getenv(AGENT_ENV_VAR) or DEFAULT_AGENT


def launch_agent(project_dir: Path, agent: str | None = None) -> None:
    """Launch a coding agent in the given project directory.

    Reads the project's CLAUDE.md and passes it as appended system prompt
    so the agent has full context about the project structure and
    dataset generation patterns.

    Args:
        project_dir: Root of the generated project.
        agent: CLI command for the coding agent (e.g. "claude", "isaac").
               Falls back to MOCK_AND_ROLL_AGENT env var, then "claude".

    Raises:
        FileNotFoundError: If the agent CLI is not found in PATH.
    """
    agent_cmd = resolve_agent(agent)
    agent_bin = shutil.which(agent_cmd)
    if agent_bin is None:
        raise FileNotFoundError(
            f"Coding agent '{agent_cmd}' not found in PATH. "
            f"Set --agent or the {AGENT_ENV_VAR} environment variable, "
            "or use --no-ai to skip AI-assisted design."
        )

    claude_md = project_dir / "CLAUDE.md"
    cmd = [agent_bin]

    if claude_md.exists():
        system_prompt = claude_md.read_text()
        cmd.extend(["--append-system-prompt", system_prompt])

    subprocess.run(cmd, cwd=str(project_dir))
