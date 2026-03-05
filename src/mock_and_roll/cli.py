"""Command line interface for dataset spec and table generation."""

from __future__ import annotations

import argparse
from pathlib import Path

from .databricks_writer import write_dataset_to_delta, write_model_to_delta
from .generator import DatasetGenerator
from .spec import dump_model_spec, dump_spec, load_model_spec, load_spec
from .templates import suggest_model_spec, suggest_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mock-and-roll",
        description="Generate synthetic sample datasets and write Delta tables on Databricks.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    suggest = subparsers.add_parser("suggest", help="Suggest a dataset spec from natural language.")
    suggest.add_argument("--description", required=True, help="Free-text dataset description.")
    suggest.add_argument("--catalog", required=True, help="Target Databricks catalog.")
    suggest.add_argument("--schema", required=True, help="Target Databricks schema.")
    suggest.add_argument("--table", help="Optional table name override.")
    suggest.add_argument("--rows", type=int, default=1000, help="Row count for generated sample data.")
    suggest.add_argument("--output", help="Optional path to write YAML spec.")

    suggest_model = subparsers.add_parser("suggest-model", help="Suggest a multi-table model spec from natural language.")
    suggest_model.add_argument("--description", required=True, help="Free-text description of the target domain/model.")
    suggest_model.add_argument("--catalog", required=True, help="Target Databricks catalog.")
    suggest_model.add_argument("--schema", required=True, help="Target Databricks schema.")
    suggest_model.add_argument("--rows", type=int, default=1000, help="Base row count for model sizing.")
    suggest_model.add_argument("--output", help="Optional path to write YAML model spec.")

    preview = subparsers.add_parser("preview", help="Preview generated rows from a dataset spec.")
    preview.add_argument("--spec", required=True, help="Path to YAML dataset spec.")
    preview.add_argument("--rows", type=int, help="Optional row count override.")
    preview.add_argument("--limit", type=int, default=10, help="Rows to print.")

    preview_model = subparsers.add_parser("preview-model", help="Preview generated rows for each dataset in a model spec.")
    preview_model.add_argument("--spec", required=True, help="Path to YAML data model spec.")
    preview_model.add_argument("--limit", type=int, default=5, help="Rows to print per dataset.")

    create = subparsers.add_parser("create", help="Generate data and create a Delta table.")
    create.add_argument("--spec", required=True, help="Path to YAML dataset spec.")
    create.add_argument("--rows", type=int, help="Optional row count override.")
    create.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Write mode.")
    create.add_argument("--profile", help="Databricks CLI profile override.")
    create.add_argument("--create-catalog", action="store_true", help="Create catalog if missing.")
    create.add_argument("--no-create-schema", action="store_true", help="Do not auto-create schema.")

    create_model = subparsers.add_parser("create-model", help="Generate and create all Delta tables for a model spec.")
    create_model.add_argument("--spec", required=True, help="Path to YAML data model spec.")
    create_model.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Write mode.")
    create_model.add_argument("--profile", help="Databricks CLI profile override.")
    create_model.add_argument("--create-catalog", action="store_true", help="Create catalog if missing.")
    create_model.add_argument("--no-create-schema", action="store_true", help="Do not auto-create schema.")

    return parser


def run_suggest(args: argparse.Namespace) -> int:
    spec = suggest_spec(
        description=args.description,
        catalog=args.catalog,
        schema=args.schema,
        table=args.table,
        rows=args.rows,
    )
    spec_yaml = dump_spec(spec)
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(spec_yaml, encoding="utf-8")
        print(f"Wrote spec to {output_path}")
        return 0
    print(spec_yaml)
    return 0


def run_suggest_model(args: argparse.Namespace) -> int:
    model_spec = suggest_model_spec(
        description=args.description,
        catalog=args.catalog,
        schema=args.schema,
        rows=args.rows,
    )
    model_yaml = dump_model_spec(model_spec)
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(model_yaml, encoding="utf-8")
        print(f"Wrote model spec to {output_path}")
        return 0
    print(model_yaml)
    return 0


def run_preview(args: argparse.Namespace) -> int:
    spec = load_spec(args.spec)
    generator = DatasetGenerator()
    frame = generator.generate(spec, rows=args.rows)
    print(frame.head(args.limit).to_string(index=False))
    return 0


def run_preview_model(args: argparse.Namespace) -> int:
    model_spec = load_model_spec(args.spec)
    generator = DatasetGenerator()
    frames = generator.generate_model(model_spec)

    for dataset in model_spec.datasets:
        frame = frames.get(dataset.name)
        if frame is None:
            continue
        print(f"\n=== {dataset.name} ({len(frame)} rows) ===")
        print(frame.head(args.limit).to_string(index=False))
    return 0


def run_create(args: argparse.Namespace) -> int:
    spec = load_spec(args.spec)
    generator = DatasetGenerator()
    frame = generator.generate(spec, rows=args.rows)

    table_name = write_dataset_to_delta(
        spec=spec,
        dataframe=frame,
        mode=args.mode,
        profile=args.profile,
        create_catalog=args.create_catalog,
        create_schema=not args.no_create_schema,
    )
    print(f"Created Delta table: {table_name}")
    print(f"Rows written: {len(frame)}")
    return 0


def run_create_model(args: argparse.Namespace) -> int:
    model_spec = load_model_spec(args.spec)
    generator = DatasetGenerator()
    frames = generator.generate_model(model_spec)

    table_names = write_model_to_delta(
        model_spec=model_spec,
        frames_by_dataset=frames,
        mode=args.mode,
        profile=args.profile,
        create_catalog=args.create_catalog,
        create_schema=not args.no_create_schema,
    )
    print("Created Delta tables:")
    for table_name in table_names:
        print(f"  - {table_name}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "suggest":
        return run_suggest(args)
    if args.command == "suggest-model":
        return run_suggest_model(args)
    if args.command == "preview":
        return run_preview(args)
    if args.command == "preview-model":
        return run_preview_model(args)
    if args.command == "create":
        return run_create(args)
    if args.command == "create-model":
        return run_create_model(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
