# Claude Skill Requirements (Global + Repo CLI Access)

## Objective

Make the `databricks-dataset-generator` skill available in any Claude Code session for this user, while ensuring the skill can reliably execute the `mock-and-roll` CLI from this repository.

## Requirements

1. Global discoverability:
The skill must be loadable as a personal skill from `~/.claude/skills/<skill-name>/SKILL.md`.

2. Stable CLI execution from any working directory:
Skill commands must run the repo CLI without depending on Claude Code being launched from the repo root.

3. Predictable permissions:
Only the command patterns needed by this skill should be auto-approved while the skill is active.

4. Low-maintenance setup:
Installation should be one command and should stay linked to the repo version of the skill.

## Implementation in This Repo

1. Updated skill frontmatter:
`skills/databricks-dataset-generator/SKILL.md` now declares:
- `allowed-tools: Read, Grep, Glob, Bash(uv --project * run mock-and-roll *), Bash(databricks *)`

2. Updated skill commands:
All `uv run mock-and-roll ...` commands now use:
- `uv --project "$(realpath "${CLAUDE_SKILL_DIR}/../..")" run mock-and-roll ...`

This resolves the repository root relative to the skill directory and works when the skill is symlinked into `~/.claude/skills`.

3. Installer script:
`scripts/install-claude-skill.sh` creates/updates:
- `~/.claude/skills/databricks-dataset-generator -> <repo>/skills/databricks-dataset-generator`

## Optional User Settings Reinforcement

If needed, add these to `~/.claude/settings.json`:
- `permissions.additionalDirectories` including this repo path
- `permissions.allow` rule `Bash(uv --project * run mock-and-roll *)`

## Acceptance Criteria

1. Running `scripts/install-claude-skill.sh` creates a symlink at `~/.claude/skills/databricks-dataset-generator`.
2. The skill appears in Claude Code as a personal skill.
3. Invoking the skill can run `suggest`, `preview`, and `create` from outside the repo directory.
4. The skill no longer depends on implicit current working directory behavior.
