# Makefile for mock-and-roll

.PHONY: all help install clean test suggest-spec preview-spec create-table suggest-model preview-model create-model

VENV_DIR := .venv
UV := uv
PYTEST := $(UV) run pytest

all: help

help:
	@echo "Available commands:"
	@echo "  make install             - Install dependencies."
	@echo "  make clean               - Clean up build artifacts and caches."
	@echo "  make test                - Run all current unit tests."
	@echo "  make suggest-spec        - Suggest dataset spec from DESCRIPTION."
	@echo "  make preview-spec        - Preview generated rows from SPEC file."
	@echo "  make create-table        - Create Delta table from SPEC file."
	@echo "  make suggest-model       - Bootstrap templated model spec from DESCRIPTION (optional)."
	@echo "  make preview-model       - Preview generated rows for each dataset in MODEL_SPEC."
	@echo "  make create-model        - Create all Delta tables from MODEL_SPEC."

install:
	@echo "Installing dependencies..."
	$(UV) sync 
	@echo "Installation complete."

clean:
	@echo "Cleaning up build artifacts and caches..."
	rm -rf $(VENV_DIR)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	@echo "Cleanup complete."

test:
	@echo "Running unit tests..."
	$(PYTEST) tests/unit/test_mock_and_roll_* -q

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
	@echo "Bootstrapping a connected model spec template (manual edits expected)..."
	$(UV) run mock-and-roll suggest-model --description "$(DESCRIPTION)" --catalog "$(CATALOG)" --schema "$(SCHEMA)" --output "$(OUTPUT)"

preview-model:
	@echo "Previewing generated model datasets..."
	$(UV) run mock-and-roll preview-model --spec "$(MODEL_SPEC)"

create-model:
	@echo "Creating Delta tables from model spec..."
	$(UV) run mock-and-roll create-model --spec "$(MODEL_SPEC)"
