# Spec Guide

Use this reference when editing a dataset spec before creating the Delta table.

## Top-level fields

- `name`: Friendly dataset label.
- `description`: Human description of the sample data.
- `catalog`: Databricks catalog.
- `schema`: Databricks schema.
- `table`: Target Delta table name.
- `rows`: Number of rows to generate.
- `columns`: List of column definitions.

## Model spec fields

- `name`: Data model name.
- `description`: Data model description.
- `datasets`: List of dataset specs (same fields as top-level dataset spec).
- `relationships`: Join definitions between datasets.

Relationship item fields:
- `from_dataset`: Child dataset name.
- `from_column`: Child FK column name.
- `to_dataset`: Parent dataset name.
- `to_column`: Parent key column name.

## Column fields

- `name`: SQL-safe column name (`[A-Za-z_][A-Za-z0-9_]*`).
- `type`: `string`, `int`, `double`, `date`, `timestamp`, or `boolean`.
- `generator`: Provider function or utility generator.
- `args`: Optional arguments for that generator.

## Supported generators

- `person.identifier` (example arg: `mask: "ORD-########"`)
- `person.full_name`
- `person.email`
- `address.city`
- `address.country`
- `finance.company`
- `finance.price` (args: `minimum`, `maximum`)
- `numeric.integer_number` (args: `start`, `end`)
- `numeric.float_number` (args: `start`, `end`)
- `datetime.date` (args: `start`, `end`)
- `datetime.datetime` (args: `start`, `end`)
- `choice.choice` (args: `items: [a, b, c]`)
- `sequence.int` (args: `start`, `step`)
- `sequence.string` (args: `prefix`, `width`)

## Example

```yaml
name: sales_dataset
description: Ecommerce orders with customer and payment details
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
  - name: customer_email
    type: string
    generator: person.email
  - name: amount
    type: double
    generator: finance.price
    args:
      minimum: 10
      maximum: 1000
```

## Model example

```yaml
name: collaboration_model
description: Collaboration analytics model
datasets:
  - name: users
    description: User dimension
    catalog: dev
    schema: sandbox
    table: users
    rows: 500
    columns:
      - name: user_id
        type: string
        generator: sequence.string
        args:
          prefix: USR-
          width: 7
      - name: email
        type: string
        generator: person.email
  - name: events
    description: User events
    catalog: dev
    schema: sandbox
    table: events
    rows: 5000
    columns:
      - name: event_id
        type: string
        generator: sequence.string
        args:
          prefix: EVT-
          width: 9
      - name: user_id
        type: string
        generator: sequence.string
        args:
          prefix: USR-
          width: 7
relationships:
  - from_dataset: events
    from_column: user_id
    to_dataset: users
    to_column: user_id
```
