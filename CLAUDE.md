# AI Context for AI-Native Databricks Demo Framework

# Project Information
project:
  name: "AI-Native Demo Framework for Databricks Solution Architects"
  repository: "mock-and-roll"
  type: "databricks-demo-framework"
  purpose: "Generate synthetic data pipelines for client demonstrations using AI-powered development"

# Agent Persona and Roles
agent_persona:
  role: "Highly-experienced data engineer specializing in AI-accelerated development"
  responsibilities: "Generate data pipelines, write code, refactor, configure, manage synthetic data generation"
  tone: "Precise, reliable, self-documenting, AI-native approach"
  focus: "Precision and reliability for synthetic data pipeline generation"

# Development Environment Setup
environment:
  python_version: "3.12+"
  package_manager: "uv"
  virtual_env: ".venv"
  setup_commands:
    - "source .venv/bin/activate"

# Key Development Commands
commands:
  activate_venv: "source .venv/bin/activate"
  install_dependencies: "uv sync"
  install_editable: "uv pip install -e ."
  run_tests: "python -m pytest"
  run_demo_module: "python -m demos.<demo_name>.main"
  run_demo_with_overrides: "python -m demos.<demo_name>.main --schema custom_schema --catalog custom_catalog --records 500"

# Coding Conventions and Architecture

## Core Import Patterns (NO src prefix - handled by pyproject.toml)
Import core modules without 'src' prefix. Use the simplified import pattern for cleaner code:

### Simplified Import Pattern (PREFERRED):
```python
# Import config separately
from config import get_config, Config

# Import all core functions from the core module
from core import *
```

### Legacy Import Pattern (still supported):
```python
from core import (
    get_spark, setup_logging, get_logger,
    parse_demo_args, get_demo_args_with_config,
    ensure_catalog_schema_volume,
    save_datamodel_to_volume, batch_load_datamodel_from_volume,
    save_to_volume, batch_load_with_copy_into, get_bronze_table_name,
    Dataset, DataModel, get_workspace_schema_url
)
```

## Function Invocation Patterns
Follow these prescriptive patterns for core functionality:

### CLI Argument Parsing (REQUIRED for all new demos):
```python
from core import parse_demo_args, get_demo_args_with_config

# Standard usage - parse CLI arguments for demo
cli_overrides = parse_demo_args("Sales Demo Pipeline")

# Convenience function - parse CLI args and load config in one step
cli_overrides, config = get_demo_args_with_config("Sales Demo Pipeline")

# With custom arguments (for demo-specific parameters)
custom_args = [("--batch-size", int, "Override batch size")]
cli_overrides = parse_demo_args("Custom Demo Pipeline", custom_args)
```

### Configuration Loading:
```python
from config import get_config, Config
from core import setup_logging, get_logger

# Load configuration first
config: Config = get_config()

# Setup centralized logging using configured level
setup_logging(level=config.logging.level, include_timestamp=True, include_module=True)
logger = get_logger(__name__)

# Access nested attributes: config.databricks.catalog, config.data_generation.default_records
```

### Spark Session Initialization:
```python
from core import get_spark
spark = get_spark()  # Handles DatabricksSession vs serverless fallback automatically
```

### Resource Management:
```python
from core import ensure_catalog_schema_volume
ready: bool = ensure_catalog_schema_volume(
    spark=spark,
    catalog_name=config.databricks.catalog,
    schema_name=config.databricks.schema,
    volume_name=config.databricks.volume,
    auto_create_catalog=config.databricks.auto_create_catalog,
    auto_create_schema=config.databricks.auto_create_schema,
    auto_create_volume=config.databricks.auto_create_volume
)
```

### Data I/O Operations:
```python
# High-level operations with DataModel (PREFERRED for multiple datasets)
from core import Dataset, DataModel, save_datamodel_to_volume, batch_load_datamodel_from_volume

# Create structured datasets
datasets = [
    Dataset(name="customers", data=customers_df),
    Dataset(name="orders", data=orders_df, subdirectory="transactions")
]
data_model = DataModel(datasets=datasets)

# Save all datasets to volume
saved_paths = save_datamodel_to_volume(
    spark=spark,
    data_model=data_model,
    config=config,
    base_subdirectory="raw"
)

# Load all datasets to Delta tables (creates bronze tables with _bronze suffix)
loaded_dfs = batch_load_datamodel_from_volume(
    spark=spark,
    data_model=data_model,
    config=config,
    source_subdirectory="raw",
    drop_tables_if_exist=True
)
```

## Core Function Signatures and Return Types

### Configuration Functions
```python
from config import get_config, Config
config: Config = get_config()  # Returns: Config object with nested attributes
```

### Spark Functions
```python
from core import get_spark
spark: SparkSession = get_spark()  # Returns: SparkSession instance
```

### Catalog Functions
```python
from core import ensure_catalog_schema_volume
ready: bool = ensure_catalog_schema_volume(...)  # Returns: bool (True if successful)
```

### Data Model Classes
```python
from core import Dataset, DataModel

# Dataset class constructor
dataset = Dataset(
    name: str,                    # Required: dataset name
    data: pd.DataFrame,           # Required: pandas DataFrame
    subdirectory: Optional[str] = None  # Optional: subdirectory path
)

# DataModel class constructor  
data_model = DataModel(
    datasets: List[Dataset],      # Required: list of Dataset instances
    base_path: Optional[str] = None  # Optional: base path for datasets
)

# Dataset methods
dataset.get_file_path(base_path: str) -> str  # Returns: complete file path

# DataModel methods
data_model.get_dataset(name: str) -> Optional[Dataset]  # Returns: Dataset or None
```

### I/O Functions
```python
from core.io import (
    save_to_volume, 
    save_datamodel_to_volume, 
    batch_load_with_copy_into,
    batch_load_datamodel_from_volume,
    get_bronze_table_name
)

# Single dataset save
save_to_volume(
    spark: SparkSession,
    df: pd.DataFrame | DataFrame,  # Pandas or PySpark DataFrame
    file_path: str,
) -> None  # Returns: None

# Multiple datasets save via DataModel
save_datamodel_to_volume(
    spark: SparkSession,
    data_model: DataModel,
    config: Config,
    base_subdirectory: str = "raw"
) -> List[str]  # Returns: List of saved file paths

# Single table batch load
batch_load_with_copy_into(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    file_format: str = "PARQUET",
    table_schema: StructType = None,
    drop_table_if_exists: bool = False,
    copy_options: dict = None
) -> DataFrame  # Returns: PySpark DataFrame (SELECT * FROM target_table)

# Multiple datasets batch load via DataModel (creates bronze tables)
batch_load_datamodel_from_volume(
    spark: SparkSession,
    data_model: DataModel,
    config: Config,
    source_subdirectory: str = "raw",
    drop_tables_if_exist: bool = False
) -> List[DataFrame]  # Returns: List of PySpark DataFrames

# Table naming utility
get_bronze_table_name(
    dataset_name: str
) -> str  # Returns: Bronze table name with _bronze suffix
```

### Logging Functions
```python
from core.logging_config import setup_logging, get_logger

setup_logging(
    level: str = "INFO",
    include_timestamp: bool = True,
    include_module: bool = True
) -> None  # Returns: None

logger = get_logger(__name__)  # Returns: Logger instance
```

### CLI Functions
```python
from core.cli import parse_demo_args, get_demo_args_with_config

# Parse CLI arguments for demo
cli_overrides: Dict[str, Any] = parse_demo_args(
    description: str = "Demo Pipeline",
    custom_args: Optional[List[Tuple[str, type, str]]] = None
)  # Returns: Dict of CLI arguments with None values filtered out

# Parse CLI and load config in one step
cli_overrides, config = get_demo_args_with_config(
    description: str = "Demo Pipeline",
    custom_args: Optional[List[Tuple[str, type, str]]] = None
)  # Returns: Tuple of (cli_overrides_dict, config_object)
```

### Workspace Functions
```python
from core.workspace import get_workspace_schema_url
workspace_url: str = get_workspace_schema_url(config)  # Returns: Databricks workspace URL string
```

# Data Pipeline Architecture

## Pipeline Orchestrator Pattern (main.py)
Sequence: CLI parsing → logging setup → data generation → batch pipeline → error handling

**ALWAYS start main.py with:**
```python
from config import get_config
from core import *

from .datasets import generate_datamodel

def main():
    # Parse command line arguments using centralized parsing (REQUIRED)
    cli_overrides = parse_demo_args("Your Demo Pipeline Description")
    
    # Load configuration with CLI overrides
    config = get_config(cli_overrides=cli_overrides)
    
    # Setup centralized logging using configured level
    setup_logging(level=config.logging.level)
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting pipeline...")
        
        if cli_overrides:
            logger.info(f"CLI overrides provided: {cli_overrides}")
        
        # Your pipeline logic here
        
        logger.info("Pipeline completed successfully")
        
        # Generate and display workspace URL for easy access
        try:
            workspace_url = get_workspace_schema_url(config)
            logger.info(f"Data available at: {workspace_url}")
        except Exception as url_error:
            logger.warning(f"Could not generate workspace URL: {str(url_error)}")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
```

## Synthetic Data Generation Guidelines
- Use mimesis library for realistic synthetic datasets
- Create multiple related datasets that can be joined together and collectively form a data model.
- **CRITICAL**: Use DataModel and Dataset classes for structured data handling
- Write to Databricks Volume path in configured format.
- **CRITICAL**: Read ai_docs/mimesis_usage_guide.md to avoid common issues
- **CRITICAL**: Use the mimesis provider api reference below, between the tags: <mimesis_provider_api_reference> </mimesis_provider_api_reference>

Example:
```python
from typing import List
import random
import pandas as pd
from mimesis import Person, Finance, Datetime, Numeric, Address, Choice
from mimesis.locales import Locale

from core import DataModel, Dataset
from config import Config

def generate_user_profiles(num_records=100) -> pd.DataFrame:
    """Generate user profile dataset"""
    person = Person(Locale.EN)
    dt = Datetime()
    address = Address()
    choice = Choice()

    return pd.DataFrame({
        'user_id': [person.identifier() for _ in range(num_records)],
        'full_name': [person.full_name() for _ in range(num_records)],
        'first_name': [person.first_name() for _ in range(num_records)],
        'last_name': [person.last_name() for _ in range(num_records)],
        'phone': [person.phone_number() for _ in range(num_records)],
        'address': [address.address() for _ in range(num_records)],
        'city': [address.city() for _ in range(num_records)],
        'state': [address.state() for _ in range(num_records)],
        'postal_code': [address.postal_code() for _ in range(num_records)],
        'email': [person.email() for _ in range(num_records)],
        'signup_date': [dt.date(start=2020, end=2025) for _ in range(num_records)],
        'customer_type': [choice.choice(['individual', 'business']) for _ in range(num_records)]
    })

def generate_sales_data(user_ids: List[str], num_records=500) -> pd.DataFrame:
    """Generate product sales dataset with user_id as foreign key"""
    finance = Finance()
    dt = Datetime()
    person = Person()
    numeric = Numeric()
    choice = Choice()
    
    return pd.DataFrame({
        'transaction_id': [person.identifier(mask='TXN-########') for _ in range(num_records)],
        'user_id': [random.choice(user_ids) for _ in range(num_records)],  # Common join key
        'product': [finance.company() for _ in range(num_records)],
        'amount': [finance.price(minimum=10, maximum=1000) for _ in range(num_records)],
        'sale_date': [dt.date(start=2024, end=2025) for _ in range(num_records)],
        'quantity': [numeric.integer_number(start=1, end=10) for _ in range(num_records)],
        'unit_price': [numeric.float_number(start=10, end=1000) for _ in range(num_records)],
        'payment_method': [choice.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']) for _ in range(num_records)]
    })

def generate_datamodel(config: Config, num_records: int = 1000) -> DataModel:
    """
    Generate all datasets for the demo.
    
    This creates a star schema with user profiles as the dimension table
    and sales transactions as the fact table.
    """
    # Use configured number of records or override
    base_records = num_records or config.data_generation.default_records
    
    # Scale datasets appropriately
    num_sales = base_records
    num_users = base_records // 5  # Average 5 transactions per user
    
    # Generate users first to get their IDs
    users_data = generate_user_profiles(num_users)
    users_dataset = Dataset(
        name="user_profiles",
        data=users_data
    )
    
    # Generate sales data using actual user IDs from the users dataset
    sales_data = generate_sales_data(
        user_ids=users_data['user_id'].tolist(),
        num_records=num_sales
    )
    sales_dataset = Dataset(
        name="product_sales",
        data=sales_data
    )
    
    # Organize into DataModel
    # AI Agent Instructions: This DataModel provides a complete star schema
    # with proper foreign key relationships for realistic retail analytics
    return DataModel(
        datasets=[users_dataset, sales_dataset]
    )
```

## Medallion Architecture Implementation
- Bronze: raw data from volumes
- Silver: cleansed and transformed
- Gold: business-ready views and aggregations

## Batch Ingestion
- Use the batch copy into function in core.io for ingestion
- Load from Volume → write to Delta table
- Follow example in @src/examples/sales_demo

# Configuration Management

## Multi-Layer Configuration System
Precedence (lowest to highest):
1. `config/base.yml` - Base defaults
2. `config/environments/*.yml` - Environment overrides  
3. `.env` - Team defaults (committed)
4. `.env.local` - Personal overrides (gitignored)
5. Environment variables - Runtime/deployment
6. CLI arguments - Command line overrides (highest priority)

## Key Environment Variables
Essential vars: ENVIRONMENT, DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DATABRICKS_WORKSPACE_URL

## CLI Configuration Overrides
Support for runtime configuration overrides via command line arguments:
- `--schema SCHEMA` - Override Databricks schema name
- `--catalog CATALOG` - Override Databricks catalog name  
- `--volume VOLUME` - Override Databricks volume name
- `--records RECORDS` - Override number of records to generate
- `--log-level LOG_LEVEL` - Override the logging level

Examples:
```bash
# Use custom schema
python -m examples.sales_demo --schema my_test_schema

# Multiple overrides
python -m examples.sales_demo --schema test --catalog dev_catalog --records 500

# Help for available options
python -m examples.sales_demo --help
```

CLI arguments take highest precedence, overriding all other configuration sources.

# AI-Native Development Patterns

## Agent-Optimized Patterns
When generating new demos, follow these patterns:
- Embedded prompts in docstrings guide AI development
**CRITICAL**: When building new demos test them end to end.

## Workspace URL Presentation for AI Agents
**CRITICAL**: When running demos, AI agents MUST capture and present the workspace URL to users:
- The workspace URL is logged at the end of successful pipeline execution
- Look for log messages containing "Data available at: https://..."
- **ALWAYS present this URL to the user** when a demo completes successfully
- Format: "Your data is now available in Databricks at: [URL]"
- This allows users to immediately explore their generated data in the Databricks interface
- If the URL generation fails, inform the user that data was created but workspace URL could not be generated

## Client Demonstration Workflow
1. Discovery: Understand client's data architecture
2. Generation: Use AI to create matching synthetic datasets
3. Pipeline Creation: Generate ingestion, transformation, analytics code

# Critical Lessons Learned

## Mimesis Date Handling
**CRITICAL**: Common issue with mimesis date generation
- `dt.date(start=2020, end=2025)` and `dt.datetime(start=2024, end=2025)` expects INTEGER years, not datetime objects. I.e. if you are using python native datetime objects, then extract the year before providing to the mimesis provider e.g.
```python
transaction_date = generic.datetime.datetime(
            start=datetime.now().year - 1,
            end=datetime.now().year
        )
```
- When using start_date/end_date variables, ensure they're integers or convert properly
- Read ai_docs/mimesis_usage_guide.md before implementing date generation

## Mimesis Numeric Handling
- The numeric data providers like `generic.numeric.float_number`, `generic.numeric.integer_number`, `generic.numeric.floats` and `generic.numeric.integers` all use `start=` and `end=` as params. DO NOT use `min=` , `max=`. 

## Error Handling
- **ALWAYS** use centralized logging from `core.logging_config`
- Setup logging at the start of every main.py: `setup_logging(level=config.logging.level)`
- Get module-specific loggers: `logger = get_logger(__name__)`
- Return appropriate exit codes on failures
- Handle both datetime.date and datetime.datetime object conflicts
- **CRITICAL**: If you're having repeated troubles (say ~5x failures) with silver or gold layer tables you may try removing them from the pipeline and running a simpler pipeline finishes without error. 

# Development Workflow

## When Creating New Demos
1. If the user doesn't specify, ask for a dataset type such as the source of data to mimic or a use case that needs to be fulfilled.
2. Clarify by asking the user questions if their specified requirements are vague before code generation
3. **MANDATORY**: Use centralized CLI parsing with `from core import parse_demo_args`
4. **Use the core and config modules in src**. E.g. use logging_config.py for logging. Use spark.py for setting up spark sessions. Etc.
5. Use mimesis for realistic synthetic data generation
6. **Use DataModel/Dataset pattern** for multiple related datasets
7. Execute new scripts as modules: `python -m demos.<demo_name>` (assuming __main__.py has been implemented) or `python -m demos.<demo_name>.main`
8. Test thoroughly with realistic data volumes. After building the code, run the demo end-to-end and attempt to fix any errors encountered.

**CLI Parsing Requirement**: ALL new demos MUST use `parse_demo_args()` from `core` to ensure consistent CLI interface across all demos. This provides standard arguments (--schema, --catalog, --volume, --records, --format, --log-level) automatically.

## File Organization
- New demos go in `src/demos/<demo_name>/` directory
- Follow existing implementation patterns of `src/examples` for example `src/examples/sales_demo` dir
- **REQUIRED**: Each new demo directory must contain exactly these 4 files:
  1. `__init__.py` - Empty Python package file
  2. `__main__.py` - Entry point for running the demo as a module
  3. `main.py` - Main orchestrator and pipeline execution
  4. `datasets.py` - Dataset generation and data model definitions
- Do not create additional files unless explicitly required

# Testing and Validation
- Test demos end-to-end before client presentations. After building the code, run the demo end-to-end and attempt to fix any errors encountered.

# Dependencies and Versions
- Python 3.12+ required
- databricks-connect>=16.4.0
- mimesis>=18.0.0
- pytest>=8.4.0
- python-dotenv>=1.0.0
- pyyaml>=6.0.2
- pydantic>=2.0.0 (for Dataset and DataModel classes)

# Interaction Guidelines
- Ask for clarification when requirements are ambiguous
- Confirm all changes before applying
- Make sure to understand client use case before generating demos
- Focus on creating realistic, industry-specific synthetic datasets
- Prioritize invisible configuration - demos should "just work"

# Documentation Resources

## Mimesis Provider API Reference
<mimesis_provider_api_reference>
# Providers API Reference

This document provides a comprehensive reference of all available provider classes and their methods for synthetic data generation.

## Core Usage Pattern
```python
from providers.generic import Generic
generic = Generic()

# Access any provider via generic object
address_data = generic.address.address()
person_name = generic.person.full_name()
random_price = generic.finance.price(minimum=10, maximum=1000)
```

## Provider Classes

### Address Provider (`generic.address`)
Generates geographic and location data.

**Key Methods:**
- `address()` - Full street address
- `city()` - City name
- `state(abbr=False)` - State/province
- `postal_code()` - ZIP/postal code
- `country()` - Country name
- `coordinates(dms=False)` - Latitude/longitude
- `street_number(maximum=1400)` - Street number
- `calling_code()` - Phone country code

### Person Provider (`generic.person`)
Generates personal information and demographics.

**Key Methods:**
- `full_name(gender=None)` - Complete name
- `first_name(gender=None)` - Given name
- `last_name(gender=None)` - Family name
- `email(domains=None, unique=False)` - Email address
- `phone_number(mask="")` - Phone number
- `birthdate(min_year=1980, max_year=2023)` - Date of birth
- `occupation()` - Job/profession
- `username()` - Username
- `password(length=8, hashed=False)` - Password
- `identifier(mask="##-##/##")` - Custom ID pattern

### Finance Provider (`generic.finance`)
Generates financial and business data.

**Key Methods:**
- `company()` - Company name
- `price(minimum=500, maximum=1500)` - Monetary amount
- `currency_iso_code()` - Currency code (USD, EUR, etc.)
- `bank()` - Bank name
- `stock_ticker()` - Stock symbol
- `stock_name()` - Company stock name
- `cryptocurrency_symbol()` - Crypto symbol

### Datetime Provider (`generic.datetime`)
Generates dates, times, and temporal data.

**Key Methods:**
- `date(start=2000, end=current_year)` - Date object
- `datetime(start=current_year, end=current_year)` - Datetime object
- `time()` - Time object
- `timestamp(fmt=TimestampFormat.POSIX)` - Unix timestamp
- `formatted_date(fmt="")` - Custom date format
- `day_of_week(abbr=False)` - Weekday name
- `month(abbr=False)` - Month name
- `year(minimum=1990, maximum=current_year)` - Year

### Numeric Provider (`generic.numeric`)
Generates numerical data and sequences.

**Key Methods:**
- `integer_number(start=-1000, end=1000)` - Random integer
- `float_number(start=-1000.0, end=1000.0)` - Random float
- `integers(start=0, end=10, n=10)` - List of integers
- `floats(start=0, end=1, n=10)` - List of floats
- `decimal_number(start=-1000.0, end=1000.0)` - Decimal number
- `matrix(m=10, n=10, num_type=NumType.FLOAT)` - Numerical matrix

### Choice Provider (`generic.choice`)
Selects random items from sequences.

**Key Methods:**
- `choice(*args)` - Single random choice
- `__call__(items, length=0, unique=False)` - Multiple random choices
- Note that the choice doesn't support weights like Python native random library.

### Internet Provider (`generic.internet`)
Generates web and network-related data.

**Key Methods:**
- `url(scheme=URLScheme.HTTPS)` - Web URL
- `email()` - Email address
- `ip_v4()` - IPv4 address
- `ip_v6()` - IPv6 address
- `mac_address()` - MAC address
- `hostname()` - Domain hostname
- `user_agent()` - Browser user agent
- `port()` - Network port number

### Text Provider (`generic.text`)
Generates textual content and strings.

**Key Methods:**
- `text(quantity=5)` - Multiple sentences
- `sentence()` - Single sentence
- `words(quantity=5)` - List of words
- `word()` - Single word
- `title()` - Title text
- `quote()` - Famous quote
- `color()` - Color name
- `hex_color(safe=False)` - Hex color code

### Hardware Provider (`generic.hardware`)
Generates computer and device specifications.

**Key Methods:**
- `cpu()` - Processor name
- `ram_size()` - Memory size
- `graphics()` - Graphics card
- `manufacturer()` - Hardware maker
- `phone_model()` - Mobile device model
- `resolution()` - Screen resolution

### Transport Provider (`generic.transport`)
Generates vehicle and transportation data.

**Key Methods:**
- `manufacturer()` - Vehicle manufacturer
- `car()` - Car model
- `airplane()` - Aircraft model
- `vehicle_registration_code()` - License plate format

### Payment Provider (`generic.payment`)
Generates payment and financial account data.

**Key Methods:**
- `credit_card_number(card_type=None)` - Credit card number
- `credit_card_expiration_date()` - Expiry date
- `cvv()` - Security code
- `paypal()` - PayPal account
- `bitcoin_address()` - Crypto wallet address

### Food Provider (`generic.food`)
Generates food and beverage data.

**Key Methods:**
- `dish()` - Food dish name
- `fruit()` - Fruit/berry name
- `vegetable()` - Vegetable name
- `drink()` - Beverage name
- `spices()` - Spice/herb name

### Development Provider (`generic.development`)
Generates software development data.

**Key Methods:**
- `programming_language()` - Language name
- `version()` - Semantic version
- `boolean()` - True/False
- `os()` - Operating system
- `software_license()` - License type

### File Provider (`generic.file`)
Generates file and filesystem data.

**Key Methods:**
- `file_name(file_type=None)` - Filename with extension
- `extension(file_type=None)` - File extension
- `mime_type(type_=None)` - MIME type
- `size(minimum=1, maximum=100)` - File size string

### Cryptographic Provider (`generic.cryptographic`)
Generates cryptographic and security data.

**Key Methods:**
- `uuid()` - UUID string
- `hash(algorithm=None)` - Hash value
- `token_hex(entropy=32)` - Hex token
- `token_urlsafe(entropy=32)` - URL-safe token

### Code Provider (`generic.code`)
Generates codes and identifiers.

**Key Methods:**
- `isbn()` - ISBN number
- `issn(mask="####-####")` - ISSN number
- `pin(mask="####")` - PIN code
- `ean()` - EAN barcode

### Science Provider (`generic.science`)
Generates scientific data.

**Key Methods:**
- `dna_sequence(length=10)` - DNA sequence
- `rna_sequence(length=10)` - RNA sequence
- `measure_unit(name=None)` - SI unit

### Path Provider (`generic.path`)
Generates filesystem paths.

**Key Methods:**
- `root()` - Root directory
- `home()` - Home directory
- `user()` - User path
- `project_dir()` - Project directory

### BinaryFile Provider (`generic.binaryfile`)
Generates binary file content.

**Key Methods:**
- `image(file_type=ImageFile.PNG)` - Image bytes
- `video(file_type=VideoFile.MP4)` - Video bytes
- `audio(file_type=AudioFile.MP3)` - Audio bytes
- `document(file_type=DocumentFile.PDF)` - Document bytes

## Common Patterns

### Date Generation
```python
# Use integer years, not datetime objects
transaction_date = generic.datetime.date(start=2020, end=2025)
recent_datetime = generic.datetime.datetime(start=2024, end=2025)
```

### Numeric Ranges
```python
# Use start/end parameters, not min/max
price = generic.numeric.float_number(start=10.0, end=1000.0)
quantity = generic.numeric.integer_number(start=1, end=100)
```

### Related Data Generation
```python
# Generate related datasets with foreign keys
users = []
for i in range(100):
    user_id = generic.person.identifier()
    users.append({
        'user_id': user_id,
        'name': generic.person.full_name(),
        'email': generic.person.email()
    })

# Use user_ids for related transactions
transactions = []
for i in range(500):
    transactions.append({
        'transaction_id': generic.person.identifier(),
        'user_id': random.choice([u['user_id'] for u in users]),
        'amount': generic.finance.price(minimum=10, maximum=1000)
    })
```
</mimesis_provider_api_reference>


# Extensions and Tools
If you need additional capabilities beyond the core framework:
- Consider web search tools for researching industry-specific patterns
- Use MCP servers for additional data generation or processing capabilities

# Important Instructions!
- **CRITICAL**: When creating demos Do NOT change any code in the @src/core module or in @src/config module, just edit the code in the @src/demos directory
- Keep any silver and gold layer transformations simple unless specifically asked for more complexity.