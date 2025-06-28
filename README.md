
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

## 🤖 How AI Builds Demos So Easily

This framework is designed specifically for AI coding assistants. Here's why Claude and other AI tools can generate complete synthetic data pipelines in seconds:

### 1. **CLAUDE.md/.goosehints - The AI's Instruction Manual**
The repository contains a comprehensive `CLAUDE.md` (identical to `.goosehints`) file that serves as an AI context document. This file:
- Provides complete code patterns and examples for every common task
- Documents all available functions with their exact signatures and return types
- Contains best practices and common pitfalls (like [mimesis](https://mimesis.name/master/) date handling)
- Includes working examples of synthetic data generation patterns

### 2. **Pre-built Core Functions**
The `src/core/` directory provides battle-tested building blocks:
- **`spark.py`**: Handles Spark session creation automatically
- **`io.py`**: Provides high-level data I/O operations (save/load to volumes)
- **`catalog.py`**: Manages Databricks resources (catalogs, schemas, volumes)
- **`cli.py`**: Standardized CLI argument parsing for all demos
- **`data.py`**: Pydantic models (Dataset, DataModel) for structured data handling

### 3. **Standardized Demo Structure**
Every demo follows the same 4-file pattern:
```
demos/your_demo/
├── __init__.py      # Package marker
├── __main__.py      # Entry point
├── main.py          # Pipeline orchestrator
└── datasets.py      # Synthetic data generation using mimesis
```
_[What is mimesis?](https://mimesis.name/master/)_

### 4. **AI-Optimized Workflow**
When you ask Claude or Goose to create a demo, it:
1. Reads the CLAUDE.md or .goosehints for patterns and best practices
2. Uses the core functions as building blocks (no reinventing the wheel)
3. Follows the standardized structure (always knows where code goes)
4. Leverages embedded examples for synthetic data generation
5. Applies learned lessons 
6. Tests and iterates on the code to ensure it runs

This design means AI can focus on your specific business logic rather than infrastructure, resulting in working demos in minutes instead of hours.

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