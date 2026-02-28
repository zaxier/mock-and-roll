# Makefile for mock-and-roll

.PHONY: all help install clean test test-unit test-integration

VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
UV := uv
PYTEST := $(PYTHON) -m pytest

all: help

help:
	@echo "Available commands:"
	@echo "  make install          - Install dependencies and project in editable mode"
	@echo "  make clean            - Clean up build artifacts and caches"
	@echo "  make test             - Run all tests"
	@echo "  make test-unit        - Run unit tests"
	@echo "  make test-integration - Run integration tests"

install: $(VENV_DIR)
	@echo "Installing dependencies..."
	$(UV) sync --all-extras
	@echo "Installation complete."

$(VENV_DIR):
	@echo "Creating virtual environment..."
	python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created."

clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR) dist build *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	@echo "Cleanup complete."

test:
	@echo "Running all tests..."
	$(PYTEST)

test-unit:
	@echo "Running unit tests..."
	$(PYTEST) tests/unit/

test-integration:
	@echo "Running integration tests..."
	$(PYTEST) tests/integration/
