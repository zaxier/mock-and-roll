# AI Context for mock-and-roll Development

## What is mock-and-roll?

A pip-installable CLI tool that scaffolds synthetic data pipeline projects for Databricks.
Users run `mock-and-roll create <name>`, get a scaffolded project, optionally design datasets
with Claude Code, then `mock-and-roll build` to produce a standalone wheel.

## Repository Structure

```
mock-and-roll/
├── pyproject.toml                  # CLI tool packaging (click, jinja2, build)
├── src/mock_and_roll/
│   ├── __init__.py                 # __version__ = "1.0.0"
│   ├── __main__.py                 # python -m mock_and_roll
│   ├── cli.py                      # Click CLI: create, build, design, run
│   ├── scaffold.py                 # Jinja2 template rendering + bundled_core copying
│   ├── builder.py                  # python -m build subprocess
│   ├── ai_session.py              # Claude Code subprocess launcher
│   ├── bundled_core/              # Core modules copied into generated projects
│   │   ├── __init__.py            # Re-exports all public API
│   │   ├── config.py              # Flat dataclass Config + argparse + env vars
│   │   ├── spark.py               # get_spark()
│   │   ├── catalog.py             # ensure_catalog_schema_volume()
│   │   ├── io.py                  # save/load operations (batch only, no streaming)
│   │   ├── data.py                # Dataset, DataModel (pydantic)
│   │   ├── logging_config.py      # setup_logging, get_logger
│   │   └── workspace.py           # get_workspace_schema_url()
│   └── templates/                 # Jinja2 templates for generated projects
│       ├── pyproject.toml.j2
│       ├── __init__.py.j2
│       ├── __main__.py.j2
│       ├── main.py.j2             # Pipeline orchestrator template
│       ├── datasets.py.j2         # Placeholder for AI to fill in
│       ├── README.md.j2
│       ├── CLAUDE.md.j2           # AI context for dataset design
│       ├── .env.example.j2
│       └── .gitignore.j2
├── tests/
│   ├── conftest.py
│   ├── unit/
│   └── integration/
├── ai_docs/                        # Reference docs (content embedded in CLAUDE.md.j2)
├── CLAUDE.md                       # This file
├── README.md
└── Makefile
```

## Key Design Decisions

### bundled_core vs templates
- `bundled_core/` modules are **copied verbatim** into generated projects as `core/`
- `templates/` are **rendered with Jinja2** (project name, package name substituted)
- bundled_core uses **relative imports** (e.g., `from .logging_config import get_logger`)

### Config simplification
- Old: Multi-layer YAML + env + CLI with nested dataclasses (DatabricksConfig, etc.)
- New: Single flat `@dataclass Config` with ~80 lines. Resolution: defaults -> env vars -> CLI.
- Access: `config.catalog` instead of `config.databricks.catalog`
- Volume path: `config.get_volume_path("raw/file")` instead of `config.databricks.get_volume_path()`

### No streaming/DLT in bundled_core
- `io.py` only has batch operations: save_to_volume, batch_load_with_copy_into, etc.
- No autoloader, no DLT, no streaming imports

## CLI Commands

- `mock-and-roll create <name>` - Scaffold project + launch Claude Code
- `mock-and-roll build` - Build wheel (shells to `python -m build`)
- `mock-and-roll design` - Relaunch Claude Code for existing project
- `mock-and-roll run` - Run project locally

## Development

```bash
uv sync --all-extras
make test
```

## Testing

```bash
pytest tests/unit/              # Unit tests (no Databricks needed)
pytest tests/integration/       # Integration tests (scaffold + build)
```

## When modifying bundled_core

If you change a module in `bundled_core/`, remember:
1. All imports must be relative (from `.module import thing`)
2. Config references use flat attributes (`config.catalog`, not `config.databricks.catalog`)
3. No dependencies beyond what generated projects declare (databricks-connect, mimesis, pandas, pydantic)
4. Update `bundled_core/__init__.py` if you add/remove public API

## When modifying templates

Templates use Jinja2 with these variables:
- `{{ project_name }}` - As provided by user (may contain hyphens)
- `{{ package_name }}` - Python-safe (hyphens replaced with underscores)
- `{{ catalog }}`, `{{ schema }}`, `{{ records }}` - Defaults from create command
