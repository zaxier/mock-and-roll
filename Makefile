# Makefile for mock-and-roll

.PHONY: all help install clean test test-unit test-integration test-spark test-databricks run-demo add-dep auth-databricks show-config drop-schema sync-docs suggest-spec preview-spec create-table suggest-model preview-model create-model

VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
UV := uv
PYTEST := $(PYTHON) -m pytest

all: help

help:
	@echo "Available commands:"
	@echo "  make install             - Install Python dependencies and project in editable mode."
	@echo "  make clean               - Clean up build artifacts and caches."
	@echo "  make test                - Run all tests."
	@echo "  make test-unit           - Run unit tests."
	@echo "  make test-integration    - Run integration tests."
	@echo "  make test-spark          - Run Spark-dependent tests."
	@echo "  make test-databricks     - Run Databricks-dependent tests."
	@echo "  make show-config         - Display the current configuration."
	@echo "  make drop-schema         - Drop a Databricks schema."
	@echo "  make sync-docs           - Synchronize documentation."
	@echo "  make suggest-spec        - Suggest dataset spec from DESCRIPTION."
	@echo "  make preview-spec        - Preview generated rows from SPEC file."
	@echo "  make create-table        - Create Delta table from SPEC file."
	@echo "  make suggest-model       - Suggest connected model spec from DESCRIPTION."
	@echo "  make preview-model       - Preview generated rows for each dataset in MODEL_SPEC."
	@echo "  make create-model        - Create all Delta tables from MODEL_SPEC."

install: $(VENV_DIR)
	@echo "Installing dependencies and project in editable mode..."
	$(UV) sync 
	@echo "Installation complete."

$(VENV_DIR):
	@echo "Creating virtual environment..."
	python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created."

clean:
	@echo "Cleaning up build artifacts and caches..."
	rm -rf $(VENV_DIR)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	@echo "Cleanup complete."

test:
	@echo "Running all tests..."
	$(PYTEST)

test-unit:
	@echo "Running unit tests..."
	$(PYTEST) -m unit

test-integration:
	@echo "Running integration tests..."
	$(PYTEST) -m integration

test-spark:
	@echo "Running Spark-dependent tests..."
	$(PYTEST) -m spark

test-databricks:
	@echo "Running Databricks-dependent tests..."
	$(PYTEST) -m databricks

show-config:
	@echo "Displaying current configuration..."
	$(PYTHON) scripts/show_config.py

drop-schema:
	@echo "Dropping Databricks schema..."
	$(PYTHON) scripts/drop_schema.py

sync-docs:
	@echo "Synchronizing documentation..."
	$(PYTHON) scripts/sync_docs.py

suggest-spec:
	@echo "Suggesting a dataset spec..."
	$(UV) run mock-and-roll suggest --description "$(DESCRIPTION)" --catalog "$(CATALOG)" --schema "$(SCHEMA)" --output "$(OUTPUT)"

preview-spec:
	@echo "Previewing a generated dataset..."
	$(UV) run mock-and-roll preview --spec "$(SPEC)"

create-table:
	@echo "Creating Delta table from spec..."
	$(UV) run mock-and-roll create --spec "$(SPEC)"

suggest-model:
	@echo "Suggesting a connected model spec..."
	$(UV) run mock-and-roll suggest-model --description "$(DESCRIPTION)" --catalog "$(CATALOG)" --schema "$(SCHEMA)" --output "$(OUTPUT)"

preview-model:
	@echo "Previewing generated model datasets..."
	$(UV) run mock-and-roll preview-model --spec "$(MODEL_SPEC)"

create-model:
	@echo "Creating Delta tables from model spec..."
	$(UV) run mock-and-roll create-model --spec "$(MODEL_SPEC)"
