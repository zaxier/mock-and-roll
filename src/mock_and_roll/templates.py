"""Description-to-spec and description-to-model suggestion heuristics."""

from __future__ import annotations

import re
from typing import List, Tuple

from .spec import ColumnSpec, DataModelSpec, DatasetSpec, RelationshipSpec


TemplateColumns = List[ColumnSpec]


BASE_COLUMNS: TemplateColumns = [
    ColumnSpec(name="record_id", type="string", generator="person.identifier", args={"mask": "REC-########"}),
    ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2024, "end": 2026}),
]

SALES_COLUMNS: TemplateColumns = BASE_COLUMNS + [
    ColumnSpec(name="customer_name", type="string", generator="person.full_name"),
    ColumnSpec(name="customer_email", type="string", generator="person.email"),
    ColumnSpec(name="product_name", type="string", generator="finance.company"),
    ColumnSpec(name="quantity", type="int", generator="numeric.integer_number", args={"start": 1, "end": 8}),
    ColumnSpec(name="unit_price", type="double", generator="numeric.float_number", args={"start": 9.99, "end": 699.0}),
    ColumnSpec(name="payment_method", type="string", generator="choice.choice", args={"items": ["card", "paypal", "cash"]}),
    ColumnSpec(name="order_date", type="date", generator="datetime.date", args={"start": 2024, "end": 2026}),
]

FINANCE_COLUMNS: TemplateColumns = BASE_COLUMNS + [
    ColumnSpec(name="account_id", type="string", generator="person.identifier", args={"mask": "ACC-########"}),
    ColumnSpec(name="customer_name", type="string", generator="person.full_name"),
    ColumnSpec(name="currency_code", type="string", generator="choice.choice", args={"items": ["USD", "AUD", "EUR", "GBP"]}),
    ColumnSpec(name="transaction_amount", type="double", generator="finance.price", args={"minimum": 10, "maximum": 25000}),
    ColumnSpec(name="merchant_name", type="string", generator="finance.company"),
    ColumnSpec(name="transaction_type", type="string", generator="choice.choice", args={"items": ["purchase", "refund", "transfer"]}),
    ColumnSpec(name="risk_flag", type="boolean", generator="choice.choice", args={"items": [True, False, False, False]}),
]

HEALTH_COLUMNS: TemplateColumns = BASE_COLUMNS + [
    ColumnSpec(name="patient_id", type="string", generator="person.identifier", args={"mask": "PAT-########"}),
    ColumnSpec(name="patient_name", type="string", generator="person.full_name"),
    ColumnSpec(name="provider_name", type="string", generator="person.full_name"),
    ColumnSpec(name="facility_city", type="string", generator="address.city"),
    ColumnSpec(name="diagnosis_group", type="string", generator="choice.choice", args={"items": ["respiratory", "cardiology", "orthopedic", "preventive"]}),
    ColumnSpec(name="visit_cost", type="double", generator="finance.price", args={"minimum": 85, "maximum": 4800}),
    ColumnSpec(name="admitted_on", type="date", generator="datetime.date", args={"start": 2024, "end": 2026}),
]

GENERIC_COLUMNS: TemplateColumns = BASE_COLUMNS + [
    ColumnSpec(name="name", type="string", generator="person.full_name"),
    ColumnSpec(name="email", type="string", generator="person.email"),
    ColumnSpec(name="city", type="string", generator="address.city"),
    ColumnSpec(name="country", type="string", generator="address.country"),
    ColumnSpec(name="amount", type="double", generator="finance.price", args={"minimum": 10, "maximum": 1000}),
]

TEMPLATES: List[Tuple[str, Tuple[str, ...], TemplateColumns]] = [
    ("sales", ("sales", "order", "retail", "ecommerce", "customer"), SALES_COLUMNS),
    ("finance", ("finance", "transaction", "payments", "bank", "fraud"), FINANCE_COLUMNS),
    ("health", ("health", "patient", "clinic", "hospital"), HEALTH_COLUMNS),
]


def slugify(text: str) -> str:
    """Create a valid identifier from free text."""
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    if not slug:
        slug = "sample_dataset"
    if slug[0].isdigit():
        slug = f"dataset_{slug}"
    return slug[:63]


def choose_template(description: str) -> Tuple[str, TemplateColumns]:
    """Pick a template based on keywords in the user description."""
    lowered = description.lower()
    for template_name, keywords, columns in TEMPLATES:
        if any(keyword in lowered for keyword in keywords):
            return template_name, columns
    return "generic", GENERIC_COLUMNS


def suggest_spec(
    description: str,
    catalog: str,
    schema: str,
    table: str | None = None,
    rows: int = 1000,
) -> DatasetSpec:
    """Create a first-pass dataset spec from description + defaults."""
    template_name, columns = choose_template(description)
    table_name = table or slugify(description)
    return DatasetSpec(
        name=f"{template_name}_dataset",
        description=description.strip(),
        catalog=catalog,
        schema=schema,
        table=table_name,
        rows=rows,
        columns=columns,
    )


def _atlassian_model_spec(description: str, catalog: str, schema: str, rows: int) -> DataModelSpec:
    user_rows = max(200, rows // 5)
    workspace_rows = max(20, rows // 50)
    jira_project_rows = max(50, rows // 10)
    jira_issue_rows = max(rows, 500)
    jira_event_rows = max(rows * 3, 1500)
    confluence_space_rows = max(40, rows // 12)
    confluence_page_rows = max(rows // 2, 300)
    confluence_event_rows = max(rows * 2, 800)

    datasets = [
        DatasetSpec(
            name="users",
            description="Directory of Atlassian users",
            catalog=catalog,
            schema=schema,
            table="users",
            rows=user_rows,
            columns=[
                ColumnSpec(name="user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="full_name", type="string", generator="person.full_name"),
                ColumnSpec(name="email", type="string", generator="person.email"),
                ColumnSpec(name="department", type="string", generator="choice.choice", args={"items": ["engineering", "product", "support", "security"]}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2022, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="workspaces",
            description="Atlassian cloud workspaces",
            catalog=catalog,
            schema=schema,
            table="workspaces",
            rows=workspace_rows,
            columns=[
                ColumnSpec(name="workspace_id", type="string", generator="sequence.string", args={"prefix": "WS-", "width": 5}),
                ColumnSpec(name="workspace_name", type="string", generator="finance.company"),
                ColumnSpec(name="plan_tier", type="string", generator="choice.choice", args={"items": ["free", "standard", "premium", "enterprise"]}),
                ColumnSpec(name="region", type="string", generator="choice.choice", args={"items": ["us-east", "us-west", "eu-central", "ap-southeast"]}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2021, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="jira_projects",
            description="Projects hosted in Jira",
            catalog=catalog,
            schema=schema,
            table="jira_projects",
            rows=jira_project_rows,
            columns=[
                ColumnSpec(name="project_id", type="string", generator="sequence.string", args={"prefix": "PROJ-", "width": 6}),
                ColumnSpec(name="workspace_id", type="string", generator="sequence.string", args={"prefix": "WS-", "width": 5}),
                ColumnSpec(name="project_key", type="string", generator="sequence.string", args={"prefix": "PJ", "width": 4}),
                ColumnSpec(name="project_name", type="string", generator="finance.company"),
                ColumnSpec(name="project_type", type="string", generator="choice.choice", args={"items": ["software", "business", "service_management"]}),
                ColumnSpec(name="owner_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2022, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="jira_issues",
            description="Issue records in Jira projects",
            catalog=catalog,
            schema=schema,
            table="jira_issues",
            rows=jira_issue_rows,
            columns=[
                ColumnSpec(name="issue_id", type="string", generator="sequence.string", args={"prefix": "ISS-", "width": 9}),
                ColumnSpec(name="project_id", type="string", generator="sequence.string", args={"prefix": "PROJ-", "width": 6}),
                ColumnSpec(name="reporter_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="assignee_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="issue_type", type="string", generator="choice.choice", args={"items": ["story", "task", "bug", "epic"]}),
                ColumnSpec(name="status", type="string", generator="choice.choice", args={"items": ["backlog", "in_progress", "review", "done"]}),
                ColumnSpec(name="priority", type="string", generator="choice.choice", args={"items": ["low", "medium", "high", "critical"]}),
                ColumnSpec(name="story_points", type="double", generator="choice.choice", args={"items": [1.0, 2.0, 3.0, 5.0, 8.0, 13.0]}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
                ColumnSpec(name="resolved_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="jira_issue_events",
            description="User interactions and status transitions for Jira issues",
            catalog=catalog,
            schema=schema,
            table="jira_issue_events",
            rows=jira_event_rows,
            columns=[
                ColumnSpec(name="event_id", type="string", generator="sequence.string", args={"prefix": "JEV-", "width": 10}),
                ColumnSpec(name="issue_id", type="string", generator="sequence.string", args={"prefix": "ISS-", "width": 9}),
                ColumnSpec(name="actor_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="event_type", type="string", generator="choice.choice", args={"items": ["comment_added", "status_changed", "field_updated", "work_logged"]}),
                ColumnSpec(name="old_status", type="string", generator="choice.choice", args={"items": ["backlog", "in_progress", "review", "done"]}),
                ColumnSpec(name="new_status", type="string", generator="choice.choice", args={"items": ["backlog", "in_progress", "review", "done"]}),
                ColumnSpec(name="event_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="confluence_spaces",
            description="Confluence spaces linked to workspaces",
            catalog=catalog,
            schema=schema,
            table="confluence_spaces",
            rows=confluence_space_rows,
            columns=[
                ColumnSpec(name="space_id", type="string", generator="sequence.string", args={"prefix": "SPC-", "width": 6}),
                ColumnSpec(name="workspace_id", type="string", generator="sequence.string", args={"prefix": "WS-", "width": 5}),
                ColumnSpec(name="space_key", type="string", generator="sequence.string", args={"prefix": "SP", "width": 4}),
                ColumnSpec(name="space_name", type="string", generator="finance.company"),
                ColumnSpec(name="created_by_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2022, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="confluence_pages",
            description="Pages in Confluence spaces",
            catalog=catalog,
            schema=schema,
            table="confluence_pages",
            rows=confluence_page_rows,
            columns=[
                ColumnSpec(name="page_id", type="string", generator="sequence.string", args={"prefix": "PAGE-", "width": 8}),
                ColumnSpec(name="space_id", type="string", generator="sequence.string", args={"prefix": "SPC-", "width": 6}),
                ColumnSpec(name="author_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="title", type="string", generator="finance.company"),
                ColumnSpec(name="page_type", type="string", generator="choice.choice", args={"items": ["article", "meeting_notes", "runbook", "design_doc"]}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
                ColumnSpec(name="updated_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="confluence_page_events",
            description="Confluence page behavioral events",
            catalog=catalog,
            schema=schema,
            table="confluence_page_events",
            rows=confluence_event_rows,
            columns=[
                ColumnSpec(name="event_id", type="string", generator="sequence.string", args={"prefix": "CEV-", "width": 10}),
                ColumnSpec(name="page_id", type="string", generator="sequence.string", args={"prefix": "PAGE-", "width": 8}),
                ColumnSpec(name="actor_user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="event_type", type="string", generator="choice.choice", args={"items": ["page_view", "comment_added", "page_edited", "mention_added"]}),
                ColumnSpec(name="session_id", type="string", generator="sequence.string", args={"prefix": "SES-", "width": 9}),
                ColumnSpec(name="event_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
            ],
        ),
    ]

    relationships = [
        RelationshipSpec(from_dataset="jira_projects", from_column="workspace_id", to_dataset="workspaces", to_column="workspace_id"),
        RelationshipSpec(from_dataset="jira_projects", from_column="owner_user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="jira_issues", from_column="project_id", to_dataset="jira_projects", to_column="project_id"),
        RelationshipSpec(from_dataset="jira_issues", from_column="reporter_user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="jira_issues", from_column="assignee_user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="jira_issue_events", from_column="issue_id", to_dataset="jira_issues", to_column="issue_id"),
        RelationshipSpec(from_dataset="jira_issue_events", from_column="actor_user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="confluence_spaces", from_column="workspace_id", to_dataset="workspaces", to_column="workspace_id"),
        RelationshipSpec(from_dataset="confluence_spaces", from_column="created_by_user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="confluence_pages", from_column="space_id", to_dataset="confluence_spaces", to_column="space_id"),
        RelationshipSpec(from_dataset="confluence_pages", from_column="author_user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="confluence_page_events", from_column="page_id", to_dataset="confluence_pages", to_column="page_id"),
        RelationshipSpec(from_dataset="confluence_page_events", from_column="actor_user_id", to_dataset="users", to_column="user_id"),
    ]

    return DataModelSpec(
        name="atlassian_data_lake_model",
        description=description,
        datasets=datasets,
        relationships=relationships,
    )


def _generic_activity_model_spec(description: str, catalog: str, schema: str, rows: int) -> DataModelSpec:
    user_rows = max(100, rows // 4)
    account_rows = max(30, rows // 15)
    event_rows = max(rows, 300)
    session_rows = max(rows // 2, 200)

    datasets = [
        DatasetSpec(
            name="users",
            description="User dimension",
            catalog=catalog,
            schema=schema,
            table="users",
            rows=user_rows,
            columns=[
                ColumnSpec(name="user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="full_name", type="string", generator="person.full_name"),
                ColumnSpec(name="email", type="string", generator="person.email"),
                ColumnSpec(name="country", type="string", generator="address.country"),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2022, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="accounts",
            description="Account/workspace dimension",
            catalog=catalog,
            schema=schema,
            table="accounts",
            rows=account_rows,
            columns=[
                ColumnSpec(name="account_id", type="string", generator="sequence.string", args={"prefix": "ACC-", "width": 6}),
                ColumnSpec(name="account_name", type="string", generator="finance.company"),
                ColumnSpec(name="plan_tier", type="string", generator="choice.choice", args={"items": ["free", "pro", "enterprise"]}),
                ColumnSpec(name="created_at", type="timestamp", generator="datetime.datetime", args={"start": 2021, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="sessions",
            description="User sessions",
            catalog=catalog,
            schema=schema,
            table="sessions",
            rows=session_rows,
            columns=[
                ColumnSpec(name="session_id", type="string", generator="sequence.string", args={"prefix": "SES-", "width": 9}),
                ColumnSpec(name="user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="account_id", type="string", generator="sequence.string", args={"prefix": "ACC-", "width": 6}),
                ColumnSpec(name="device_type", type="string", generator="choice.choice", args={"items": ["desktop", "mobile", "tablet"]}),
                ColumnSpec(name="started_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
            ],
        ),
        DatasetSpec(
            name="events",
            description="Behavioral events",
            catalog=catalog,
            schema=schema,
            table="events",
            rows=event_rows,
            columns=[
                ColumnSpec(name="event_id", type="string", generator="sequence.string", args={"prefix": "EVT-", "width": 10}),
                ColumnSpec(name="session_id", type="string", generator="sequence.string", args={"prefix": "SES-", "width": 9}),
                ColumnSpec(name="user_id", type="string", generator="sequence.string", args={"prefix": "USR-", "width": 7}),
                ColumnSpec(name="event_type", type="string", generator="choice.choice", args={"items": ["view", "click", "create", "update", "delete"]}),
                ColumnSpec(name="object_type", type="string", generator="choice.choice", args={"items": ["dashboard", "ticket", "page", "comment"]}),
                ColumnSpec(name="event_at", type="timestamp", generator="datetime.datetime", args={"start": 2023, "end": 2026}),
            ],
        ),
    ]

    relationships = [
        RelationshipSpec(from_dataset="sessions", from_column="user_id", to_dataset="users", to_column="user_id"),
        RelationshipSpec(from_dataset="sessions", from_column="account_id", to_dataset="accounts", to_column="account_id"),
        RelationshipSpec(from_dataset="events", from_column="session_id", to_dataset="sessions", to_column="session_id"),
        RelationshipSpec(from_dataset="events", from_column="user_id", to_dataset="users", to_column="user_id"),
    ]

    return DataModelSpec(
        name="activity_data_model",
        description=description,
        datasets=datasets,
        relationships=relationships,
    )


def suggest_model_spec(description: str, catalog: str, schema: str, rows: int = 1000) -> DataModelSpec:
    """Create interconnected multi-table model spec from natural language description."""
    lowered = description.lower()
    if any(keyword in lowered for keyword in ("atlassian", "jira", "confluence")):
        return _atlassian_model_spec(description=description, catalog=catalog, schema=schema, rows=rows)
    return _generic_activity_model_spec(description=description, catalog=catalog, schema=schema, rows=rows)
