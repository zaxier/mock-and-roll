---
name: databricks-dataset-generator
description: Create synthetic sample datasets and publish them as Delta tables in Databricks using the mock-and-roll CLI. Use when a user asks for sample/fake/mock data from a natural-language description, wants a Databricks table quickly populated for demos/tests, or wants help iterating a dataset schema before writing to Delta.
---

# Databricks Dataset Generator

Generate one or more YAML specs from a user prompt, preview synthetic rows, and create Delta tables directly.

## Workflow

1. Capture a short dataset brief.
Ask for domain, expected row count, and required columns or business entities.

2. Generate a first-pass spec.
Run:
```bash
uv run mock-and-roll suggest \
  --description "<user description>" \
  --catalog <catalog> \
  --schema <schema> \
  --rows <rows> \
  --output /tmp/<table>_spec.yml
```

If the request is multi-entity and join-heavy, run:
```bash
uv run mock-and-roll suggest-model \
  --description "<user description>" \
  --catalog <catalog> \
  --schema <schema> \
  --rows <rows> \
  --output /tmp/<domain>_model.yml
```

3. Review and refine the spec/model.
Open the output YAML and adjust columns, row counts, and relationships to match the user request. Use [references/spec-guide.md](references/spec-guide.md) for supported patterns.

4. Preview sample data before writing.
Run:
```bash
uv run mock-and-roll preview --spec /tmp/<table>_spec.yml --limit 10
```
For model specs:
```bash
uv run mock-and-roll preview-model --spec /tmp/<domain>_model.yml --limit 5
```

5. Create the Delta table.
Run:
```bash
uv run mock-and-roll create --spec /tmp/<table>_spec.yml --profile <profile_name>
```
For model specs:
```bash
uv run mock-and-roll create-model --spec /tmp/<domain>_model.yml --profile <profile_name>
```

6. Report outcome.
Return full table name, row count written, and any schema adjustments made from the original prompt.

## Guardrails

- Validate identifiers before writing: catalog/schema/table must be SQL-safe names.
- Prefer `overwrite` mode unless user explicitly asks to append.
- Keep generated data synthetic and non-sensitive.
- Fail fast on Databricks auth/session errors and report the exact command to reauthenticate.
