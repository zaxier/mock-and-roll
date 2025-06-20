
# AI-Native Demo Framework for Databricks Solution Architects

> **"This is not just a repository. This is a demonstration of the future of software development."**

Transform client demonstrations with AI-generated synthetic data pipelines. Experience the exponential acceleration of AI-powered development where coding agents like Claude Code elevate software engineering from implementation to architecture.

## 🚀 Quick Start

### Zero-to-Demo in 5 Minutes
```bash
# 1. Installation
git clone <repo-url> && cd custom-demo-accelerator
uv sync && uv pip install -e .

# 2. Configuration (minimal setup)
echo "DATABRICKS_CATALOG=demo_catalog" > .env.local
echo "DATABRICKS_SCHEMA=synthetic_data" >> .env.local

# 3. Generate your first AI-powered demo
python -m reference_demos.sales_demo.main
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

### Multi-Layer Configuration System
**Precedence (Lowest → Highest):**
1. `config/base.yml` - Base defaults
2. `config/environments/{env}.yml` - Environment overrides
3. `.env` - Team defaults (committed)
4. `.env.local` - Personal overrides (gitignored)
5. Environment variables - Runtime/deployment

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
# Install in editable mode (recommended)
uv pip install -e .

# Run tests
python -m pytest

# Run specific demo
python -m netsuite_hft_demo.main

# Add new dependency
uv add <package-name>
```

## 🔗 Databricks Integration

### Authentication
```bash
databricks auth login --host <workspace-url>
```

## 🌐 The Bigger Picture

### What This Means for Data Engineering
This isn't just about building pipelines—it's about demonstrating a new paradigm where:
- **Humans become architects** powered by AI coding agents
- **Prompt engineering becomes strategic** rather than tactical
- **Exponential growth becomes achievable** through AI amplification
- **Software engineering transcends implementation** to focus on design and creativity

### Repository as Teaching Tool
This codebase demonstrates AI-native development principles:
- Configuration transparency
- Prompt-driven development
- Agent-optimized patterns
- Exponential scaling foundations

## 📚 Getting Started

See `CLAUDE.md` for detailed development guidance, architectural decisions, and AI-native development patterns.

## 🔧 Dependencies

- Python 3.12+
- `databricks-connect>=16.3.0`
- `mimesis>=18.0.0` 
- `python-dotenv>=1.0.0`

---

**Experience the future of data engineering demonstrations. Transform your client presentations with AI-generated synthetic data pipelines.**