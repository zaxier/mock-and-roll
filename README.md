# Mock and Roll

`mock-and-roll` is a CLI-first tool for generating synthetic datasets and creating
Delta tables directly in Databricks.

The project is intentionally scoped to:
- infer a dataset spec from a user description
- infer an interconnected multi-table model spec from a user description
- generate sample rows with `mimesis`
- write results straight to Delta tables

It does not implement volume ingestion pipelines or Spark transformation pipelines.

## Install

```bash
uv sync
```

## Configure Databricks Auth

```bash
databricks auth login --host <workspace_url> --profile <profile_name>
```

Optional environment setup:

```bash
echo "DATABRICKS_CONFIG_PROFILE=<profile_name>" > .env.local
```

## CLI Commands

### 1. Suggest a dataset spec

```bash
uv run mock-and-roll suggest \
  --description "Ecommerce sales orders with customer and payment fields" \
  --catalog dev \
  --schema sandbox \
  --rows 2000 \
  --output specs/orders.yml
```

### 2. Preview generated rows locally

```bash
uv run mock-and-roll preview --spec specs/orders.yml --limit 10
```

### 3. Create Delta table in Databricks

```bash
uv run mock-and-roll create --spec specs/orders.yml --profile <profile_name>
```

## Connected Data Models

For realistic domain modeling (multiple joinable tables):

```bash
uv run mock-and-roll suggest-model \
  --description "Atlassian data lake with behavioral data for Jira and Confluence" \
  --catalog dev \
  --schema sandbox \
  --rows 10000 \
  --output specs/atlassian_model.yml
```

```bash
uv run mock-and-roll preview-model --spec specs/atlassian_model.yml --limit 5
```

```bash
uv run mock-and-roll create-model --spec specs/atlassian_model.yml --profile <profile_name>
```

`preview-model` and `create-model` enforce FK-style relationships by sampling child key values from generated parent tables.

## Spec Format

```yaml
name: sales_dataset
description: Ecommerce orders
catalog: dev
schema: sandbox
table: ecommerce_orders
rows: 1000
columns:
  - name: order_id
    type: string
    generator: person.identifier
    args:
      mask: ORD-########
  - name: amount
    type: double
    generator: finance.price
    args:
      minimum: 10
      maximum: 250
```

## Developer Notes

- Entry point: [src/mock_and_roll/cli.py](/Users/xavier.armitage/Library/CloudStorage/Dropbox/Repositories/dev/mock-and-roll/src/mock_and_roll/cli.py)
- Spec models: [src/mock_and_roll/spec.py](/Users/xavier.armitage/Library/CloudStorage/Dropbox/Repositories/dev/mock-and-roll/src/mock_and_roll/spec.py)
- Generation logic: [src/mock_and_roll/generator.py](/Users/xavier.armitage/Library/CloudStorage/Dropbox/Repositories/dev/mock-and-roll/src/mock_and_roll/generator.py)
- Databricks writer: [src/mock_and_roll/databricks_writer.py](/Users/xavier.armitage/Library/CloudStorage/Dropbox/Repositories/dev/mock-and-roll/src/mock_and_roll/databricks_writer.py)
