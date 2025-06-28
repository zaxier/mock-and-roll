
# AI-Native Demo Framework for Databricks Solution Architects

Transform client demonstrations with AI-generated synthetic data pipelines.

## 🚀 Quick Start

### Requirements
- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** - Fast Python package manager
- **make** - Build automation tool (pre-installed on macOS/Linux). (Not strictly required)
- **[Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)** - For workspace authentication
- **An AI Coding Assistant**:
  - [Goose](https://github.com/square/goose) - AI-powered development agent
  - [Claude Code](https://claude.ai/code) - Anthropic's CLI coding assistant
  - Or similar AI coding tools for automated demo generation

### Zero-to-Demo in 5 Minutes
```bash
# 1. Installation
git clone https://github.com/xavier-armitage_data/mock-and-roll.git && cd mock-and-roll
make install  # or `uv sync`

# 2. Authenticate to databricks workspace
databricks auth login --host <workspace_url> --profile <profile_name>

# 3. Configuration (minimal setup)
echo "DATABRICKS_CATALOG=<your_catalog>" > .env.local
echo "DATABRICKS_SCHEMA=<your_schema>" >> .env.local
echo "DATABRICKS_CONFIG_PROFILE=<profile_name>" >> .env.local

# 4. Activate python env (or skip and  use `uv run python...` below)
source .venv/bin/activate

# 5. Familiarise yourself with the CLI args for overrides
python -m examples.sales_demo -h

# 6. Run the example pipeline
python -m examples.sales_demo --schema mock_and_roll_example
   
# 7. Create a custom demo - with Goose or Claude Code
goose -t "Create a new synthetic data pipeline for [your industry] with [specific requirements/use cases]"
# or 
claude "Create a new synthetic data pipeline for [your industry] with [specific requirements/use cases]"
```

Watch as AI generates complete data pipelines—synthetic data creation, ingestion, ETL—all orchestrated seamlessly.

## 🎯 Perfect for Databricks Solution Architects

### Why This Framework?
- **🎯 Client-Ready Demos**: Generate industry-specific synthetic datasets instantly
- **⚡ AI-Accelerated Development**: Prompt-driven pipeline creation using Goose or Claude Code
- **🏗️ Extensible Architecture**: Pre-built skeleton for rapid customization
- **📊 Realistic Data**: Mimesis-powered synthetic data that mirrors real-world scenarios

## 🔧 Framework Features

### Datagen helper functions
```python
# High-level data operations with Pydantic models
from core.data import Dataset, DataModel
from core.io import save_datamodel_to_volume, batch_load_datamodel_from_volume

# Create structured datasets
datasets = [
    Dataset(name="customers", data=customers_df),
    Dataset(name="orders", data=orders_df)
]
data_model = DataModel(datasets=datasets)

# Batch operations on multiple datasets
save_datamodel_to_volume(spark, data_model, config)
batch_load_datamodel_from_volume(spark, data_model, config)
```

## Configuration

This repository uses a layered configuration system. Settings are applied with the following precedence (where the last one wins):

1. `config/base.yml` (Base defaults for the entire project)
2. `config/environments/<ENV>.yml` (Environment-specific settings, e.g., `dev.yml`)
3. `.env` (Team-level secrets and defaults, should be committed)
4. `.env.local` (Your personal overrides, gitignored)
5. **Environment Variables** (e.g., `export DATABRICKS_SCHEMA=my_schema`)
6. **CLI Arguments** (e.g., `--schema my_schema`, highest priority)

> **Note**: For most users, you shouldn't need to modify the `config/*.yml` files. The framework is designed to work out-of-the-box with sensible defaults. Use `.env.local` for personal overrides and CLI arguments for runtime customization.

## 🏗️ Architecture Overview

### Package Structure
```
src/
├── config/               # Multi-layer configuration (invisible in demos)
├── core/                 # Reusable utilities (Spark, I/O, Catalog)
├── examples/             # Demo templates for extension
└── demos/[your_demo]/    # Generated demo pipelines
```

### Core Components
- **`core/spark.py`**: Centralized Spark session management with serverless fallback
- **`core/io.py`**: Volume I/O operations, delta writing, and high-level data operations
- **`core/data.py`**: Pydantic data models for structured data handling (Dataset, DataModel)
- **`core/catalog.py`**: Databricks resource management (catalogs, schemas, volumes)
- **`core/logging_config.py`**: Centralized logging configuration for consistent output
- **`config/`**: Multi-layer configuration system with .env support

### Auto-Creation Behavior
- **Catalogs**: Auto-creation disabled by default (requires explicit permission)
- **Schemas**: Auto-creation enabled by default (common development need)
- **Volumes**: Always auto-created if missing

## 🎨 Client Demonstration Workflow

1. **Discovery**: Understand client's data architecture and use cases
2. **Generation**: Use AI agent to create matching synthetic datasets
3. **Pipeline Creation**: Generate ingestion, transformation, and analytics code
4. **Deployment**: Auto-deploy to Databricks workspace
5. **Demonstration**: Show end-to-end capabilities with realistic data

### Example Customization
```bash
# Use Claude Code with prompts like:
"Create a synthetic dataset for pharmaceutical clinical trials"
"Generate a supply chain demo for automotive manufacturing"
"Build a customer 360 pipeline for telecommunications"
```
The more specific the better the result.

## ⚡ Development Commands

```bash
# Install dependencies and project
make install

# Run all tests
make test

# Run specific test suites
make test-unit           # Unit tests only
make test-integration    # Integration tests
make test-spark          # Spark-dependent tests
make test-databricks     # Databricks-dependent tests

# Utility commands
make show-config         # Display current configuration
make drop-schema         # Drop Databricks schema
make clean              # Clean build artifacts and caches
```

### Available Commands
Run `make help` to see all available commands, or refer to the [Development Commands](#-development-commands) section above.

### Troubleshooting
- **Virtual environment issues**: Run `make clean` then `make install`
- **Configuration problems**: Use `make show-config` to verify settings
- **Test failures**: Run specific test suites to isolate issues (`make test-unit`, `make test-spark`, etc.)

## 🔧 Dependencies

- Python 3.12+
- `databricks-connect>=16.4.0`
- `mimesis>=18.0.0` 
- `python-dotenv>=1.0.0`

---

**Experience the future of data engineering demonstrations. Transform your client presentations with AI-generated synthetic data pipelines.**