# Makefile for custom-demo-accelerator

.PHONY: all help install clean test test-unit test-integration test-spark test-databricks run-demo add-dep auth-databricks show-config drop-schema sync-docs

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
	@echo "  make run-demo DEMO=<demo_path> [ARGS='--schema custom'] - Run a specific demo (e.g., make run-demo DEMO=examples.sales_demo.main)."
	@echo "  make show-config         - Display the current configuration."
	@echo "  make drop-schema         - Drop a Databricks schema."
	@echo "  make sync-docs           - Synchronize documentation."

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

run-demo:
ifndef DEMO
	$(error DEMO is required. Usage: make run-demo DEMO=<demo_path> [ARGS='--schema custom'])
endif
	@echo "Running demo: $(DEMO) with args: $(ARGS)..."
	$(PYTHON) -m $(DEMO) $(ARGS)

show-config:
	@echo "Displaying current configuration..."
	$(PYTHON) scripts/show_config.py

drop-schema:
	@echo "Dropping Databricks schema..."
	$(PYTHON) scripts/drop_schema.py

sync-docs:
	@echo "Synchronizing documentation..."
	$(PYTHON) scripts/sync_docs.py
