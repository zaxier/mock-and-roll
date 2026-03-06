#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"

SKILL_NAME="databricks-dataset-generator"
SKILL_SRC="${REPO_ROOT}/skills/${SKILL_NAME}"
CLAUDE_HOME="${CLAUDE_HOME:-$HOME/.claude}"
SKILLS_DIR="${CLAUDE_HOME}/skills"
SKILL_DEST="${SKILLS_DIR}/${SKILL_NAME}"

if [[ ! -f "${SKILL_SRC}/SKILL.md" ]]; then
  echo "ERROR: skill not found at ${SKILL_SRC}" >&2
  exit 1
fi

mkdir -p "${SKILLS_DIR}"
ln -sfn "${SKILL_SRC}" "${SKILL_DEST}"

cat <<EOF
Installed Claude Code skill symlink:
  ${SKILL_DEST} -> ${SKILL_SRC}

Recommended permissions in ~/.claude/settings.json:
  permissions.allow += Bash(uv --project * run mock-and-roll *)
  permissions.allow += Bash(databricks *)
EOF
