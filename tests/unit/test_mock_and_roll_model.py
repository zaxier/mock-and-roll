import pytest

from mock_and_roll.generator import DatasetGenerator
from mock_and_roll.templates import suggest_model_spec


@pytest.mark.unit
def test_suggest_model_atlassian_contains_connected_tables():
    model_spec = suggest_model_spec(
        description="Atlassian data lake with Jira and Confluence behavioral data",
        catalog="dev",
        schema="sandbox",
        rows=500,
    )
    dataset_names = {dataset.name for dataset in model_spec.datasets}
    assert "users" in dataset_names
    assert "jira_issues" in dataset_names
    assert "confluence_pages" in dataset_names
    assert len(model_spec.relationships) >= 8


@pytest.mark.unit
def test_generate_model_fk_values_exist_in_parent_tables():
    model_spec = suggest_model_spec(
        description="Atlassian data lake with Jira and Confluence behavioral data",
        catalog="dev",
        schema="sandbox",
        rows=200,
    )
    generator = DatasetGenerator(seed=42)
    frames = generator.generate_model(model_spec)

    users = frames["users"]
    jira_issues = frames["jira_issues"]
    jira_events = frames["jira_issue_events"]

    user_ids = set(users["user_id"].tolist())
    issue_ids = set(jira_issues["issue_id"].tolist())

    assert set(jira_issues["reporter_user_id"]).issubset(user_ids)
    assert set(jira_issues["assignee_user_id"]).issubset(user_ids)
    assert set(jira_events["issue_id"]).issubset(issue_ids)

