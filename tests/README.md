# Test Suite Documentation

This directory contains a comprehensive test suite for the custom demo accelerator, organized into both unit tests and integration tests.

## Directory Structure

```
tests/
├── README.md                     # This file
├── conftest.py                   # Shared test fixtures and configuration
├── unit/                         # Unit tests (fast, mocked)
│   ├── __init__.py
│   ├── test_config_settings.py
│   ├── test_core_catalog_volume.py
│   ├── test_core_io.py           # Includes tests for batch_load_with_copy_into
│   └── test_core_spark.py
└── integration/                  # Integration tests (slower, real components)
    ├── __init__.py
    ├── test_core_catalog_integration.py
    ├── test_core_io_integration.py
    ├── test_end_to_end_integration.py
    └── test_simple_integration.py
```

## Test Types

### Unit Tests
- **Location**: `tests/unit/`
- **Purpose**: Test individual components in isolation using mocks
- **Speed**: Fast (< 1 second per test)
- **Dependencies**: No external services required
- **Coverage**: All core modules with comprehensive mocking

### Integration Tests
- **Location**: `tests/integration/`
- **Purpose**: Test module interactions and workflows
- **Speed**: Moderate (1-10 seconds per test)
- **Dependencies**: May require Spark session (falls back to mocking when unavailable)
- **Coverage**: Cross-module workflows and data pipelines

## Key Test Features

### New Functionality Tested

1. **`batch_load_with_copy_into` Function** (in `test_core_io.py`):
   - Basic COPY INTO functionality
   - Schema creation and table management
   - Error handling and options processing
   - Integration with various file formats
   - Performance and logging verification

2. **Module Integration Tests**:
   - Config → Data Generation integration
   - Complete datagen workflow testing
   - Autoloader pipeline integration
   - End-to-end data flow validation

3. **Error Handling & Edge Cases**:
   - Spark session failure handling
   - Catalog operation failures
   - Data quality issues
   - Resource cleanup verification

## Test Fixtures

### Shared Fixtures (conftest.py)
- `spark_session`: Attempts real Spark, falls back to core.spark
- `temp_dir`: Temporary directory for file operations
- `test_config`: Mock configuration with test-safe values
- `sample_financial_data`: Generated financial transaction data
- `financial_transactions_schema`: Schema definition for financial data
- `clean_spark_tables`: Automatic test table cleanup

## Running Tests

### Run All Tests
```bash
pytest tests/
```

### Run Only Unit Tests
```bash
pytest tests/unit/
```

### Run Only Integration Tests
```bash
pytest tests/integration/
```

### Run Tests by Marker
```bash
# Run integration tests
pytest -m integration

# Run Spark-dependent tests
pytest -m spark

# Run fast unit tests
pytest -m unit
```

### Run Specific Test Classes
```bash
# Test the new batch load functionality
pytest tests/unit/test_core_io.py::TestBatchLoadWithCopyInto -v

# Test module integration
pytest tests/integration/test_simple_integration.py::TestModuleIntegration -v
```

## Test Markers

- `@pytest.mark.integration`: Integration tests (may be slow)
- `@pytest.mark.unit`: Unit tests (fast)
- `@pytest.mark.spark`: Tests requiring Spark
- `@pytest.mark.databricks`: Tests requiring Databricks environment

## Test Coverage Areas

### Core Modules Tested
1. **core.catalog**: Catalog, schema, volume operations
2. **core.io**: File I/O, streaming, batch loading with COPY INTO
3. **core.spark**: Spark session management
4. **config**: Configuration loading and management
5. **financial_txs_demo**: Demo pipeline workflows

### Integration Scenarios Tested
1. **Data Generation Pipeline**: Config → Generate → Save
2. **Autoloader Pipeline**: Load → Transform → Save to Delta
3. **Batch Loading**: COPY INTO operations with various options
4. **End-to-End Workflows**: Complete medallion architecture simulation
5. **Error Recovery**: Failure handling and graceful degradation

## Best Practices Followed

1. **Comprehensive Mocking**: Unit tests fully isolate components
2. **Realistic Integration**: Integration tests use strategic mocking for unavailable resources
3. **Error Testing**: Both success and failure scenarios covered
4. **Performance Testing**: Timing and resource usage verification
5. **Data Quality**: Generated data validation and business rule testing
6. **Configuration Testing**: Environment-specific behavior validation

## Environment Considerations

The test suite is designed to work in various environments:

- **Local Development**: Uses mocked Spark sessions when local Spark unavailable
- **CI/CD Pipelines**: All tests can run without external dependencies
- **Databricks Environment**: Can leverage real Databricks resources when available
- **Docker/Containerized**: Temporary directories and cleanup ensure isolation

## Maintenance Notes

- Tests automatically clean up resources (temporary files, test tables)
- Mock configurations prevent interference with real environments
- Fixtures provide consistent test data across all test classes
- Performance thresholds may need adjustment based on environment capabilities