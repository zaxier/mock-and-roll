
# AI-Native Demo Framework for Databricks Solution Architects

> **"This is not just a repository. This is a demonstration of the future of software development."**

Transform client demonstrations with AI-generated synthetic data pipelines. Experience the exponential acceleration of AI-powered development where coding agents like Claude Code elevate software engineering from implementation to architecture.

## 🚀 Quick Start

### Zero-to-Demo in 5 Minutes
```bash
# 1. Installation
git clone <repo-url> && cd custom-demo-accelerator
make install

# 2. Configuration (minimal setup)
echo "DATABRICKS_CATALOG=demo_catalog" > .env.local
echo "DATABRICKS_SCHEMA=synthetic_data" >> .env.local

# 3. Generate your first AI-powered demo
python -m examples.sales_demo.main
```

### Extend with Claude Code
Open the repository in Claude Code and prompt:
> "Create a new synthetic dataset for [your industry] with [specific requirements]"

Watch as AI generates complete data pipelines—synthetic data creation, ingestion, ETL—all orchestrated seamlessly.

## 🎯 Perfect for Databricks Solution Architects

### Why This Framework?
- **🎯 Client-Ready Demos**: Generate industry-specific synthetic datasets instantly
- **⚡ AI-Accelerated Development**: Prompt-driven pipeline creation using Claude Code
- **🏗️ Extensible Architecture**: Pre-built skeleton for rapid customization
- **📊 Realistic Data**: Mimesis-powered synthetic data that mirrors real-world scenarios
- **🔧 Zero-Config Demos**: Invisible configuration management for seamless presentations

### Demo Scenarios Included

| Demo | Industry | Features | Use Case |
|------|----------|----------|----------|
| Financial Trading | Fintech | HFT, Risk Analytics, Compliance | Real-time processing, regulatory reporting |
| NetSuite ERP | Enterprise | Inventory, Customers, Transactions | ERP integration, business intelligence |
| Retail Analytics | E-commerce | Products, Orders, Customer 360 | Personalization, supply chain optimization |
| Healthcare | MedTech | Patients, Treatments, Outcomes | Clinical analytics, regulatory compliance |

## 🌟 AI-Native Architecture

### The Vision: Exponential AI Acceleration
```
**Exponential growth in AI acceleration**
     ^
     |
    ||
  .|||
```

This repository showcases how modern coding agents are transforming software engineering. The magic isn't in complexity; it's in invisible, elegant execution.

### Strategic Prompt Engineering
High-quality prompts are strategically embedded throughout the codebase, enabling:
- **Targeted Context Injection**: LLM attention mechanisms guide development decisions
- **Self-Documenting Code**: AI agents understand purpose and extend functionality
- **Exponential Scaling**: Patterns that amplify with AI capabilities

### From Coder to Architect
**The New Development Paradigm:**
- Humans focus on strategic design and architectural decisions
- AI agents handle implementation details and code generation
- Prompt engineering becomes the primary development skill
- Software engineering transcends pure logic to embrace creativity

## 🔧 Framework Features

### Invisible Configuration
Configuration operates transparently in the background, supporting agent reliability without cognitive overhead during demos.

```python
# Just import and use - configuration happens automatically
from config import get_config
from core.spark import get_spark
from core.logging_config import setup_logging
from core.data import Dataset, DataModel

setup_logging()       # Configure consistent logging
config = get_config() # Automatically loads environment
spark = get_spark()   # Handles Databricks connection
```

### Agent-Optimized Patterns
```python
# High-level data operations with Pydantic models
from core.data import Dataset, DataModel
from core.io import save_datamodel_to_volume, batch_load_datamodel_from_volume

# Create structured datasets
datasets = [
    Dataset(name="customers", data=customers_df, file_format="parquet"),
    Dataset(name="orders", data=orders_df, file_format="parquet")
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

### Multi-Layer Configuration System
**Precedence (Lowest → Highest):**
1. `config/base.yml` - Base defaults
2. `config/environments/{env}.yml` - Environment overrides
3. `.env` - Team defaults (committed)
4. `.env.local` - Personal overrides (gitignored)
5. Environment variables - Runtime/deployment
6. CLI arguments - Command line overrides

## 🏗️ Architecture Overview

### Package Structure
```
src/
├── config/          # Multi-layer configuration (invisible in demos)
├── core/            # Reusable utilities (Spark, I/O, Catalog)
├── examples/        # Demo templates for extension
└── [your_demo]/     # Generated demo pipelines
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

## 🔗 Databricks Integration

### Authentication
```bash
databricks auth login --host <workspace-url>
```
Choose a name for your profile (you'll need it again below)

## 📚 Getting Started

### Prerequisites
- Python 3.12+
- `uv` package manager installed
- Databricks workspace access (for full functionality)

### Step-by-Step Setup

1. **Clone and Install**
   ```bash
   git clone <repo-url>
   cd custom-demo-accelerator
   make install
   ```

2. **Configure Environment**
   Create your personal configuration file:
   ```bash
   # Minimal required configuration
   echo "DATABRICKS_CATALOG=your_catalog" > .env.local  # Must exist, will not be automatically created
   echo "DATABRICKS_SCHEMA=your_schema" >> .env.local  # Doesn't necessarily have to exist, will be created if allowed
   echo "DATABRICKS_CONFIG_PROFILE=your_auth_profile >> .env.local
   ```

3. **Authenticate with Databricks** (if using Databricks features)
   ```bash
   databricks auth login --host https://your-workspace.cloud.databricks.com
   ```

4. **Verify Installation**
   ```bash
   make show-config  # Display current configuration
   make test-unit    # Run unit tests
   ```

5. **Run Your First Demo**
   ```bash
   uv run python -m examples.sales_demo
   
   # Or with custom arguments
   uv run python -m examples.sales_demo --schema custom_schema --records 1000
   ```

### Available Commands
Run `make help` to see all available commands, or refer to the [Development Commands](#-development-commands) section above.

### Troubleshooting
- **Virtual environment issues**: Run `make clean` then `make install`
- **Configuration problems**: Use `make show-config` to verify settings
- **Test failures**: Run specific test suites to isolate issues (`make test-unit`, `make test-spark`, etc.)

## 🔧 Dependencies

- Python 3.12+
- `databricks-connect>=16.3.0`
- `mimesis>=18.0.0` 
- `python-dotenv>=1.0.0`

---

**Experience the future of data engineering demonstrations. Transform your client presentations with AI-generated synthetic data pipelines.**