"""Synthetic row generation from dataset and model specs."""

from __future__ import annotations

import random
from datetime import date, datetime
from collections import defaultdict, deque
from typing import Any, Dict, List

import pandas as pd
from mimesis import Address, Datetime, Finance, Numeric, Person
from mimesis.locales import Locale

from .spec import ColumnSpec, DataModelSpec, DatasetSpec


class DatasetGenerator:
    """Generate pandas DataFrames from dataset specs using Mimesis providers."""

    def __init__(self, seed: int | None = None) -> None:
        self._random = random.Random(seed)
        self.person = Person(Locale.EN)
        self.finance = Finance(Locale.EN)
        self.datetime = Datetime()
        self.numeric = Numeric()
        self.address = Address(Locale.EN)

    def generate(self, spec: DatasetSpec, rows: int | None = None) -> pd.DataFrame:
        """Generate DataFrame for a dataset spec."""
        row_count = rows or spec.rows
        records = [self._generate_record(spec.columns, idx) for idx in range(row_count)]
        return pd.DataFrame.from_records(records)

    def generate_model(self, model_spec: DataModelSpec) -> Dict[str, pd.DataFrame]:
        """
        Generate all datasets for a model and enforce FK references from relationships.

        Child FK columns are sampled from generated parent key values.
        """
        ordered_dataset_names = self._topological_order(model_spec)
        generated: Dict[str, pd.DataFrame] = {}

        for dataset_name in ordered_dataset_names:
            dataset = model_spec.get_dataset(dataset_name)
            if dataset is None:
                continue

            frame = self.generate(dataset)

            parent_relationships = [
                rel for rel in model_spec.relationships if rel.from_dataset == dataset_name
            ]
            for relationship in parent_relationships:
                parent_frame = generated.get(relationship.to_dataset)
                if parent_frame is None or relationship.to_column not in parent_frame.columns:
                    continue

                parent_values = parent_frame[relationship.to_column].dropna().tolist()
                if not parent_values:
                    continue

                frame[relationship.from_column] = [
                    self._random.choice(parent_values) for _ in range(len(frame))
                ]

            generated[dataset_name] = frame

        return generated

    def _generate_record(self, columns: List[ColumnSpec], index: int) -> Dict[str, Any]:
        record: Dict[str, Any] = {}
        for column in columns:
            value = self._generate_value(column, index)
            record[column.name] = value
        return record

    def _generate_value(self, column: ColumnSpec, index: int) -> Any:
        generator = column.generator
        args = dict(column.args)

        if generator == "choice.choice":
            items = args.get("items", [])
            if not items:
                return None
            return self._random.choice(items)

        if generator == "sequence.int":
            start = int(args.get("start", 1))
            step = int(args.get("step", 1))
            return start + (index * step)

        if generator == "sequence.string":
            prefix = str(args.get("prefix", "ID-"))
            width = int(args.get("width", 6))
            return f"{prefix}{str(index + 1).zfill(width)}"

        provider_name, _, method_name = generator.partition(".")
        provider = getattr(self, provider_name, None)
        if provider is None or not method_name:
            return self._type_fallback(column.type, index)

        method = getattr(provider, method_name, None)
        if method is None:
            return self._type_fallback(column.type, index)

        value = method(**args)
        return self._coerce_type(value, column.type, index)

    def _coerce_type(self, value: Any, column_type: str, index: int) -> Any:
        if value is None:
            return self._type_fallback(column_type, index)

        if column_type == "string":
            return str(value)
        if column_type == "int":
            return int(value)
        if column_type == "double":
            return float(value)
        if column_type == "boolean":
            return bool(value)
        if column_type == "date":
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            return date.fromisoformat(str(value))
        if column_type == "timestamp":
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime.combine(value, datetime.min.time())
            return datetime.fromisoformat(str(value))
        return value

    def _type_fallback(self, column_type: str, index: int) -> Any:
        if column_type == "string":
            return f"value_{index + 1}"
        if column_type == "int":
            return index + 1
        if column_type == "double":
            return float(index + 1)
        if column_type == "boolean":
            return (index % 2) == 0
        if column_type == "date":
            return date.today()
        if column_type == "timestamp":
            return datetime.utcnow()
        return None

    def _topological_order(self, model_spec: DataModelSpec) -> List[str]:
        dataset_names = [dataset.name for dataset in model_spec.datasets]
        edges: Dict[str, List[str]] = defaultdict(list)
        indegree: Dict[str, int] = {name: 0 for name in dataset_names}

        for relationship in model_spec.relationships:
            parent = relationship.to_dataset
            child = relationship.from_dataset
            if parent in indegree and child in indegree:
                edges[parent].append(child)
                indegree[child] += 1

        queue = deque([name for name in dataset_names if indegree[name] == 0])
        ordered: List[str] = []

        while queue:
            node = queue.popleft()
            ordered.append(node)
            for neighbor in edges[node]:
                indegree[neighbor] -= 1
                if indegree[neighbor] == 0:
                    queue.append(neighbor)

        # If cycles appear, append any remaining datasets in declared order.
        if len(ordered) != len(dataset_names):
            for name in dataset_names:
                if name not in ordered:
                    ordered.append(name)

        return ordered
