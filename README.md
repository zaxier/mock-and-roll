# mock-and-roll

AI-powered synthetic data pipeline generator for Databricks.

Generate realistic synthetic data pipelines through conversational AI, then package them as standalone wheels that run anywhere.

## Quick Start

### Install

```bash
pip install mock-and-roll
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv tool install mock-and-roll
```

### Create a project

```bash
mock-and-roll create my_retail_demo
```

This scaffolds a project and launches Claude Code for conversational dataset design. Describe your datasets in natural language and the AI generates the code.

Use `--no-ai` to scaffold without launching Claude Code:

```bash
mock-and-roll create my_retail_demo --no-ai --catalog dev --schema demo
```

### Test locally

```bash
cd my_retail_demo
mock-and-roll run
```

Or directly:

```bash
cd my_retail_demo
pip install -e .
python -m my_retail_demo --catalog dev --schema test --records 100
```

### Build a standalone wheel

```bash
mock-and-roll build
```

### Deploy anywhere

```bash
pip install dist/my_retail_demo-1.0.0-py3-none-any.whl
python -m my_retail_demo --catalog prod --schema client_demo --records 10000
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `mock-and-roll create <name>` | Scaffold a new project (+ optional AI design session) |
| `mock-and-roll build` | Build a standalone wheel from a generated project |
| `mock-and-roll design` | (Re)launch Claude Code for dataset iteration |
| `mock-and-roll run` | Run a generated project locally |

### `create` options

| Flag | Default | Description |
|------|---------|-------------|
| `--no-ai` | `false` | Skip launching Claude Code |
| `--catalog` | `dev` | Default Databricks catalog |
| `--schema` | `default` | Default Databricks schema |
| `--records` | `1000` | Default record count |
| `--output-dir` | `.` | Parent directory for the project |

## Generated Project Structure

```
my_retail_demo/
├── pyproject.toml          # Standalone build config
├── README.md
├── CLAUDE.md               # AI context for dataset design
├── .env.example
└── src/
    └── my_retail_demo/
        ├── __init__.py
        ├── __main__.py     # Entry point
        ├── main.py         # Pipeline orchestrator
        ├── datasets.py     # Dataset generation (edit this!)
        └── core/           # Framework utilities (don't edit)
```

The generated wheel has no dependency on mock-and-roll. It only needs:
- `databricks-connect>=16.4.0`
- `mimesis>=18.0.0`
- `pandas>=2.2.3`
- `pydantic>=2.0.0`

## Typical Workflow

1. **Create**: `mock-and-roll create my_demo` - scaffold + AI conversation
2. **Design**: Describe your datasets, AI generates the code in `datasets.py`
3. **Test**: `mock-and-roll run -- --records 100` - verify locally
4. **Build**: `mock-and-roll build` - produce a portable wheel
5. **Deploy**: Install the wheel in any Databricks-connected environment

## Requirements

- Python 3.12+
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) for workspace authentication
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) (optional, for AI-assisted design)

## Development

```bash
git clone https://github.com/zaxier/mock-and-roll.git && cd mock-and-roll
uv sync --all-extras
make test
```
