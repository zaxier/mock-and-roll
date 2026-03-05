"""Dataset and data-model specification models with YAML helpers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


ColumnType = Literal["string", "int", "double", "date", "timestamp", "boolean"]


class ColumnSpec(BaseModel):
    """Definition of one generated column."""

    name: str
    type: ColumnType = "string"
    generator: str
    args: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Invalid column name: {value}")
        return value


class DatasetSpec(BaseModel):
    """Dataset definition for generation and table creation."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: str = ""
    catalog: str
    schema_name: str = Field(alias="schema")
    table: str
    rows: int = 1000
    columns: List[ColumnSpec]

    @field_validator("catalog", "schema_name", "table")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Invalid identifier: {value}")
        return value

    @field_validator("rows")
    @classmethod
    def validate_rows(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("rows must be greater than 0")
        return value

    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}"


class RelationshipSpec(BaseModel):
    """Relationship definition between two datasets."""

    from_dataset: str
    from_column: str
    to_dataset: str
    to_column: str


class DataModelSpec(BaseModel):
    """Interconnected set of dataset specs and relationships."""

    name: str
    description: str = ""
    datasets: List[DatasetSpec]
    relationships: List[RelationshipSpec] = Field(default_factory=list)

    def get_dataset(self, dataset_name: str) -> Optional[DatasetSpec]:
        return next((dataset for dataset in self.datasets if dataset.name == dataset_name), None)


def load_spec(path: str | Path) -> DatasetSpec:
    """Load dataset spec from YAML file."""
    spec_path = Path(path)
    data = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    return DatasetSpec.model_validate(data)


def load_model_spec(path: str | Path) -> DataModelSpec:
    """Load multi-dataset data model spec from YAML file."""
    spec_path = Path(path)
    data = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    return DataModelSpec.model_validate(data)


def dump_spec(spec: DatasetSpec) -> str:
    """Render dataset spec to YAML string."""
    return yaml.safe_dump(
        spec.model_dump(mode="python", by_alias=True),
        sort_keys=False,
        default_flow_style=False,
    )


def dump_model_spec(model_spec: DataModelSpec) -> str:
    """Render data model spec to YAML string."""
    return yaml.safe_dump(
        model_spec.model_dump(mode="python", by_alias=True),
        sort_keys=False,
        default_flow_style=False,
    )
